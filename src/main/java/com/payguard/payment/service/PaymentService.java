package com.payguard.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.TransactionRepository;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Orchestrates the payment lifecycle.
 *
 * <p>The controlling constraint here is that <em>no remote call happens inside a database
 * transaction</em>. The previous shape wrapped the whole of {@code create} in {@code @Transactional}
 * and called the fraud engine and Stripe from inside it, which meant a Postgres connection was
 * pinned for the full duration of two network round trips (up to the 200ms fraud timeout plus
 * however long Stripe takes) — under load the pool drains and the service stops accepting requests
 * for reasons that have nothing to do with the database. Worse, if the commit failed after Stripe
 * had charged the card, the money moved and PayGuard had no record of it.
 *
 * <p>So each stage is its own short transaction, with the remote calls in between. The transaction
 * boundaries are driven through a {@link TransactionTemplate} rather than self-invoked
 * {@code @Transactional} methods, which Spring's proxying would not intercept.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final TransactionRepository tx;
    private final OutboxRepository outbox;
    private final FraudEngineClient fraud;
    private final StripeChargeService stripe;
    private final ObjectMapper json;
    private final TransactionTemplate transactionTemplate;

    public PaymentService(
            TransactionRepository tx,
            OutboxRepository outbox,
            FraudEngineClient fraud,
            StripeChargeService stripe,
            ObjectMapper json,
            PlatformTransactionManager transactionManager) {
        this.tx = tx;
        this.outbox = outbox;
        this.fraud = fraud;
        this.stripe = stripe;
        this.json = json;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public Transaction create(String merchant, long amount, String currency, String paymentToken) {
        String id = UUID.randomUUID().toString();

        // 1. Commit the intent first. If everything after this fails, the transaction still exists in
        //    PENDING and payment.created has been recorded, so the state is recoverable rather than lost.
        transactionTemplate.executeWithoutResult(status -> {
            Transaction created = tx.save(new Transaction(id, merchant, amount, currency));
            event(created, "payment.created", Map.of(
                    "event_id", UUID.randomUUID().toString(),
                    "transaction_id", id,
                    "merchant_id", merchant,
                    "amount_minor", amount,
                    "currency", currency,
                    "occurred_at", Instant.now().toString()));
        });

        // 2. Remote calls, outside any transaction.
        FraudEngineClient.Decision decision = fraud.score(id, merchant, amount, currency);

        StripeChargeService.ChargeResult charge = null;
        RuntimeException chargeFailure = null;
        if ("APPROVE".equals(decision.decision()) && hasToken(paymentToken)) {
            try {
                charge = stripe.createCharge(loadForCharge(id), paymentToken);
            } catch (RuntimeException ex) {
                // Recorded on the transaction rather than propagated raw, so a decline leaves a FAILED
                // row and a payment.failed event instead of an exception with no persisted trace.
                chargeFailure = ex;
            }
        }

        // 3. One short transaction to record the outcome.
        return applyOutcome(id, decision, charge, chargeFailure);
    }

    @Transactional(readOnly = true)
    public Transaction get(String id, String merchant) {
        return tx.findByTransactionIdAndMerchantId(id, merchant)
                .orElseThrow(() -> new NoSuchElementException("payment not found"));
    }

    /**
     * Refunds a completed payment.
     *
     * <p>The status check and the Stripe call are separated so the refund is not issued from inside a
     * transaction. Stripe is given a stable idempotency key derived from the transaction id, so a
     * retry after a crash between the call and the commit refunds once, not twice.
     */
    public Transaction refund(String id, String merchant) {
        Transaction snapshot = transactionTemplate.execute(status -> {
            Transaction found = tx.findByTransactionIdAndMerchantId(id, merchant)
                    .orElseThrow(() -> new NoSuchElementException("payment not found"));
            if (found.getStatus() != TransactionStatus.COMPLETED) {
                throw new IllegalStateException("only completed payments can be refunded");
            }
            return found;
        });

        stripe.refund(snapshot);

        return transactionTemplate.execute(status -> {
            Transaction current = tx.findByTransactionIdAndMerchantId(id, merchant)
                    .orElseThrow(() -> new NoSuchElementException("payment not found"));
            current.status(TransactionStatus.REFUNDED);
            return current;
        });
    }

    private Transaction loadForCharge(String id) {
        return transactionTemplate.execute(status -> tx.findByTransactionId(id)
                .orElseThrow(() -> new NoSuchElementException("payment not found: " + id)));
    }

    private Transaction applyOutcome(
            String id,
            FraudEngineClient.Decision decision,
            StripeChargeService.ChargeResult charge,
            RuntimeException chargeFailure) {

        return transactionTemplate.execute(status -> {
            Transaction t = tx.findByTransactionId(id)
                    .orElseThrow(() -> new NoSuchElementException("payment not found: " + id));
            t.decision(decision.score(), decision.decision());

            if (chargeFailure != null) {
                t.status(TransactionStatus.FAILED);
                publishFailed(t, failureCode(chargeFailure), failureMessage(chargeFailure));
                log.warn("Stripe charge failed for transaction {}", id, chargeFailure);
                return t;
            }

            switch (decision.decision()) {
                case "APPROVE" -> {
                    if (charge == null) {
                        // No payment token supplied: authorised but not captured.
                        t.status(TransactionStatus.APPROVED);
                    } else {
                        t.stripeChargeId(charge.chargeId());
                        t.status(TransactionStatus.COMPLETED);
                        publishCompleted(t);
                    }
                }
                case "REVIEW" -> {
                    t.status(TransactionStatus.HELD);
                    // Previously the HELD branch emitted nothing at all, so a transaction could sit in
                    // the review queue indefinitely with no downstream system — notifications,
                    // reconciliation, analytics — ever learning it existed.
                    publishHeld(t);
                }
                default -> {
                    t.status(TransactionStatus.BLOCKED);
                    publishFailed(t, "fraud_blocked", "Transaction blocked by fraud engine");
                }
            }
            return t;
        });
    }

    private static boolean hasToken(String paymentToken) {
        return paymentToken != null && !paymentToken.isBlank();
    }

    private static String failureCode(RuntimeException ex) {
        if (ex instanceof StripeChargeService.PaymentDeclinedException) {
            return "card_declined";
        }
        if (ex instanceof StripeChargeService.RetryableStripeException) {
            return "stripe_unavailable";
        }
        return "stripe_error";
    }

    private static String failureMessage(RuntimeException ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }

    private void publishCompleted(Transaction t) {
        event(t, "payment.completed", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", t.getTransactionId(),
                "merchant_id", t.getMerchantId(),
                "stripe_charge_id", t.getStripeChargeId(),
                "occurred_at", Instant.now().toString()));
    }

    private void publishHeld(Transaction t) {
        // PaymentHeld declares fraud_score and fraud_decision non-nullable; both are always set by the
        // decision(...) call that precedes this, including on the fraud-engine fallback path (score -1).
        event(t, "payment.held", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", t.getTransactionId(),
                "merchant_id", t.getMerchantId(),
                "amount_minor", t.getAmountMinor(),
                "currency", t.getCurrency(),
                "fraud_score", t.getFraudScore(),
                "fraud_decision", t.getFraudDecision(),
                "occurred_at", Instant.now().toString()));
    }

    private void publishFailed(Transaction t, String code, String message) {
        event(t, "payment.failed", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", t.getTransactionId(),
                "merchant_id", t.getMerchantId(),
                "failure_code", code,
                "failure_message", message,
                "occurred_at", Instant.now().toString()));
    }

    private void event(Transaction t, String type, Map<String, Object> payload) {
        try {
            outbox.save(new OutboxEntry(
                    (String) payload.get("event_id"),
                    t.getTransactionId(),
                    type,
                    type,
                    t.getMerchantId(),
                    json.writeValueAsString(payload)));
        } catch (Exception ex) {
            throw new IllegalStateException("cannot serialize payment event", ex);
        }
    }
}
