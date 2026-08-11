package com.payguard.payment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.ProcessedWebhook;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.ProcessedWebhookRepository;
import com.payguard.payment.repository.TransactionRepository;
import com.stripe.model.Event;
import com.stripe.model.PaymentIntent;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
public class WebhookProcessingService {

    private static final Logger log = LoggerFactory.getLogger(WebhookProcessingService.class);

    private final ProcessedWebhookRepository processed;
    private final TransactionRepository transactions;
    private final OutboxRepository outbox;
    private final ObjectMapper json;

    public WebhookProcessingService(
            ProcessedWebhookRepository processed,
            TransactionRepository transactions,
            OutboxRepository outbox,
            ObjectMapper json) {
        this.processed = processed;
        this.transactions = transactions;
        this.outbox = outbox;
        this.json = json;
    }

    /**
     * Applies one Stripe webhook event exactly once.
     *
     * <p>The dedupe row is inserted <em>before</em> the event is handled, not after. Stripe retries
     * aggressively and delivers concurrently, and with the insert last both deliveries of the same
     * event passed the {@code exists} check before either had written its marker — so a
     * {@code payment_intent.succeeded} could be applied twice and emit two
     * {@code payment.completed} events. Inserting first makes the unique index on
     * {@code stripe_event_id} the arbiter: the loser's flush fails and its whole transaction rolls
     * back, including any state change it had started to make.
     */
    @Transactional
    public void process(Event event) {
        if (processed.existsByStripeEventId(event.getId())) {
            log.debug("Ignoring already-processed Stripe event {}", event.getId());
            return;
        }
        claim(event);

        switch (event.getType()) {
            case "payment_intent.succeeded" -> handlePaymentIntentSucceeded(event);
            case "payment_intent.payment_failed" -> handlePaymentIntentFailed(event);
            case "charge.refunded" -> handleChargeRefunded(event);
            case "charge.dispute.created" -> handleDisputeCreated(event);
            default -> log.debug("Ignoring unsupported Stripe event type {}", event.getType());
        }
    }

    /**
     * Reserves this event id for the current transaction.
     *
     * <p>{@code saveAndFlush} forces the insert to hit the unique index now rather than at commit —
     * without the flush the constraint violation would only surface after the handler had already
     * mutated the transaction and queued its events.
     *
     * <p>A violation is rethrown rather than swallowed. The flush has already doomed this JPA
     * transaction, so returning normally would only make the proxy fail the commit with an opaque
     * {@code UnexpectedRollbackException}; propagating a typed exception rolls the (empty) work back
     * cleanly and lets the controller answer Stripe with a 200.
     */
    private void claim(Event event) {
        try {
            processed.saveAndFlush(new ProcessedWebhook(event.getId(), event.getType()));
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicateWebhookException(event.getId(), ex);
        }
    }

    /** Signals that a concurrent delivery of the same Stripe event won the race to claim it. */
    public static class DuplicateWebhookException extends RuntimeException {
        public DuplicateWebhookException(String stripeEventId, Throwable cause) {
            super("stripe event already being processed: " + stripeEventId, cause);
        }
    }

    private void handlePaymentIntentSucceeded(Event event) {
        PaymentIntent intent = (PaymentIntent) event.getDataObjectDeserializer()
                .getObject()
                .orElseThrow(() -> new IllegalStateException("missing payment_intent payload"));
        updateByMetadata(intent.getMetadata(), TransactionStatus.COMPLETED, intent.getLatestCharge());
    }

    private void handlePaymentIntentFailed(Event event) {
        PaymentIntent intent = (PaymentIntent) event.getDataObjectDeserializer()
                .getObject()
                .orElseThrow(() -> new IllegalStateException("missing payment_intent payload"));
        Transaction tx = findByMetadata(intent.getMetadata());
        if (tx.getStatus() == TransactionStatus.FAILED) {
            return;
        }
        tx.status(TransactionStatus.FAILED);
        publishFailed(tx, "stripe_payment_failed", "Stripe reported payment_intent.payment_failed");
    }

    private void handleChargeRefunded(Event event) {
        JsonNode charge = json.valueToTree(event.getDataObjectDeserializer().getObject().orElse(null));
        String chargeId = charge.path("id").asText(null);
        if (chargeId == null) {
            log.warn("charge.refunded event {} carried no charge id", event.getId());
            return;
        }
        transactions
                .findByStripeChargeId(chargeId)
                .ifPresentOrElse(
                        tx -> {
                            if (tx.getStatus() == TransactionStatus.REFUNDED) {
                                return;
                            }
                            tx.status(TransactionStatus.REFUNDED);
                            // A refund used to change the row and emit nothing, so reconciliation and
                            // notifications never learned the money went back.
                            publishRefunded(tx, chargeId);
                        },
                        // Silently dropping this hid genuine desynchronisation between Stripe and the
                        // ledger — a charge Stripe knows about that PayGuard has no record of.
                        () -> log.warn(
                                "charge.refunded for unknown charge {} (event {}) — no local transaction",
                                chargeId,
                                event.getId()));
    }

    private void handleDisputeCreated(Event event) {
        JsonNode dispute = json.valueToTree(event.getDataObjectDeserializer().getObject().orElse(null));
        String chargeId = dispute.path("charge").asText(null);
        if (chargeId == null) {
            log.warn("charge.dispute.created event {} carried no charge id", event.getId());
            return;
        }
        transactions
                .findByStripeChargeId(chargeId)
                .ifPresentOrElse(
                        tx -> {
                            if (tx.getStatus() != TransactionStatus.FAILED) {
                                tx.status(TransactionStatus.FAILED);
                                publishFailed(tx, "stripe_dispute", "Charge dispute opened");
                            }
                        },
                        () -> log.warn(
                                "charge.dispute.created for unknown charge {} (event {}) — no local transaction",
                                chargeId,
                                event.getId()));
    }

    private void updateByMetadata(Map<String, String> metadata, TransactionStatus status, String chargeId) {
        Transaction tx = findByMetadata(metadata);
        if (chargeId != null && !chargeId.isBlank()) {
            tx.stripeChargeId(chargeId);
        }
        if (tx.getStatus() != status) {
            tx.status(status);
            if (status == TransactionStatus.COMPLETED) {
                publishCompleted(tx);
            }
        }
    }

    private Transaction findByMetadata(Map<String, String> metadata) {
        String transactionId = metadata.get("transaction_id");
        if (transactionId == null) {
            throw new IllegalStateException("stripe event missing transaction_id metadata");
        }
        return transactions.findByTransactionId(transactionId)
                .orElseThrow(() -> new NoSuchElementException("transaction not found: " + transactionId));
    }

    private void publishCompleted(Transaction tx) {
        event(tx, "payment.completed", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", tx.getTransactionId(),
                "merchant_id", tx.getMerchantId(),
                "stripe_charge_id", tx.getStripeChargeId(),
                "occurred_at", Instant.now().toString()));
    }

    /**
     * A refund is reported on {@code payment.failed} because it is the only terminal
     * money-not-received contract the platform publishes today; the payment plan flags refund
     * reconciliation as an open contract gap.
     */
    private void publishRefunded(Transaction tx, String chargeId) {
        publishFailed(tx, "stripe_refunded", "Charge " + chargeId + " was refunded");
    }

    private void publishFailed(Transaction tx, String code, String message) {
        event(tx, "payment.failed", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", tx.getTransactionId(),
                "merchant_id", tx.getMerchantId(),
                "failure_code", code,
                "failure_message", message,
                "occurred_at", Instant.now().toString()));
    }

    private void event(Transaction tx, String type, Map<String, Object> payload) {
        try {
            outbox.save(new OutboxEntry(
                    (String) payload.get("event_id"),
                    tx.getTransactionId(),
                    type,
                    type,
                    tx.getMerchantId(),
                    json.writeValueAsString(payload)));
        } catch (Exception ex) {
            throw new IllegalStateException("cannot serialize webhook event", ex);
        }
    }
}
