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

    @Transactional
    public void process(Event event) {
        if (processed.existsByStripeEventId(event.getId())) {
            return;
        }

        switch (event.getType()) {
            case "payment_intent.succeeded" -> handlePaymentIntentSucceeded(event);
            case "payment_intent.payment_failed" -> handlePaymentIntentFailed(event);
            case "charge.refunded" -> handleChargeRefunded(event);
            case "charge.dispute.created" -> handleDisputeCreated(event);
            default -> log.debug("Ignoring unsupported Stripe event type {}", event.getType());
        }

        processed.save(new ProcessedWebhook(event.getId(), event.getType()));
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
            return;
        }
        transactions.findByStripeChargeId(chargeId).ifPresent(tx -> tx.status(TransactionStatus.REFUNDED));
    }

    private void handleDisputeCreated(Event event) {
        JsonNode dispute = json.valueToTree(event.getDataObjectDeserializer().getObject().orElse(null));
        String chargeId = dispute.path("charge").asText(null);
        if (chargeId == null) {
            return;
        }
        transactions.findByStripeChargeId(chargeId).ifPresent(tx -> {
            if (tx.getStatus() != TransactionStatus.FAILED) {
                tx.status(TransactionStatus.FAILED);
                publishFailed(tx, "stripe_dispute", "Charge dispute opened");
            }
        });
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
