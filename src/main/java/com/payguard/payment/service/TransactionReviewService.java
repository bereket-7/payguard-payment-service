package com.payguard.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionReviewAudit;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.TransactionRepository;
import com.payguard.payment.repository.TransactionReviewAuditRepository;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class TransactionReviewService {

    private final TransactionRepository transactions;
    private final TransactionReviewAuditRepository audit;
    private final OutboxRepository outbox;
    private final StripeChargeService stripe;
    private final ObjectMapper json;

    public TransactionReviewService(
            TransactionRepository transactions,
            TransactionReviewAuditRepository audit,
            OutboxRepository outbox,
            StripeChargeService stripe,
            ObjectMapper json) {
        this.transactions = transactions;
        this.audit = audit;
        this.outbox = outbox;
        this.stripe = stripe;
        this.json = json;
    }

    public List<Transaction> listHeld(String merchantId) {
        return transactions.findByMerchantIdAndStatusOrderByCreatedAtAsc(merchantId, TransactionStatus.HELD);
    }

    @Transactional
    public Transaction approve(String transactionId, String merchantId, String paymentToken, String actorSub) {
        Transaction tx = heldTransaction(transactionId, merchantId);
        StripeChargeService.ChargeResult charge = stripe.createCharge(tx, paymentToken);
        tx.stripeChargeId(charge.chargeId());
        tx.status(TransactionStatus.COMPLETED);
        audit.save(new TransactionReviewAudit(transactionId, merchantId, "APPROVE", actorSub));
        publishCompleted(tx);
        return tx;
    }

    @Transactional
    public Transaction reject(String transactionId, String merchantId, String actorSub) {
        Transaction tx = heldTransaction(transactionId, merchantId);
        tx.status(TransactionStatus.BLOCKED);
        audit.save(new TransactionReviewAudit(transactionId, merchantId, "REJECT", actorSub));
        publishFailed(tx, "manual_review_rejected", "Transaction rejected during manual review");
        return tx;
    }

    private Transaction heldTransaction(String transactionId, String merchantId) {
        Transaction tx = transactions.findByTransactionIdAndMerchantId(transactionId, merchantId)
                .orElseThrow(() -> new NoSuchElementException("transaction not found"));
        if (tx.getStatus() != TransactionStatus.HELD) {
            throw new IllegalStateException("transaction is not held for review");
        }
        return tx;
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
            throw new IllegalStateException("cannot serialize review event", ex);
        }
    }
}
