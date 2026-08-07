package com.payguard.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.TransactionRepository;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private final TransactionRepository tx;
    private final OutboxRepository outbox;
    private final FraudEngineClient fraud;
    private final StripeChargeService stripe;
    private final ObjectMapper json;

    public PaymentService(
            TransactionRepository tx,
            OutboxRepository outbox,
            FraudEngineClient fraud,
            StripeChargeService stripe,
            ObjectMapper json) {
        this.tx = tx;
        this.outbox = outbox;
        this.fraud = fraud;
        this.stripe = stripe;
        this.json = json;
    }

    @Transactional
    public Transaction create(String merchant, long amount, String currency, String paymentToken) {
        String id = UUID.randomUUID().toString();
        Transaction t = tx.save(new Transaction(id, merchant, amount, currency));
        event(t, "payment.created", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", id,
                "merchant_id", merchant,
                "amount_minor", amount,
                "currency", currency,
                "occurred_at", Instant.now().toString()));

        FraudEngineClient.Decision decision = fraud.score(id, merchant, amount, currency);
        t.decision(decision.score(), decision.decision());

        if ("APPROVE".equals(decision.decision())) {
            completeApproved(t, paymentToken);
        } else if ("REVIEW".equals(decision.decision())) {
            t.status(TransactionStatus.HELD);
        } else {
            t.status(TransactionStatus.BLOCKED);
            publishFailed(t, "fraud_blocked", "Transaction blocked by fraud engine");
        }
        return t;
    }

    @Transactional
    public Transaction get(String id, String merchant) {
        return tx.findByTransactionIdAndMerchantId(id, merchant)
                .orElseThrow(() -> new NoSuchElementException("payment not found"));
    }

    @Transactional
    public Transaction refund(String id, String merchant) {
        Transaction t = get(id, merchant);
        if (t.getStatus() != TransactionStatus.COMPLETED) {
            throw new IllegalStateException("only completed payments can be refunded");
        }
        stripe.refund(t);
        t.status(TransactionStatus.REFUNDED);
        return t;
    }

    private void completeApproved(Transaction t, String paymentToken) {
        if (paymentToken == null || paymentToken.isBlank()) {
            t.status(TransactionStatus.APPROVED);
            return;
        }
        StripeChargeService.ChargeResult charge = stripe.createCharge(t, paymentToken);
        t.stripeChargeId(charge.chargeId());
        t.status(TransactionStatus.COMPLETED);
        publishCompleted(t);
    }

    private void publishCompleted(Transaction t) {
        event(t, "payment.completed", Map.of(
                "event_id", UUID.randomUUID().toString(),
                "transaction_id", t.getTransactionId(),
                "merchant_id", t.getMerchantId(),
                "stripe_charge_id", t.getStripeChargeId(),
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
