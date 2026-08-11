package com.payguard.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionReviewAudit;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.TransactionRepository;
import com.payguard.payment.repository.TransactionReviewAuditRepository;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class TransactionReviewService {

    private static final Logger log = LoggerFactory.getLogger(TransactionReviewService.class);

    private final TransactionRepository transactions;
    private final TransactionReviewAuditRepository audit;
    private final OutboxRepository outbox;
    private final StripeChargeService stripe;
    private final ObjectMapper json;
    private final TransactionTemplate transactionTemplate;

    public TransactionReviewService(
            TransactionRepository transactions,
            TransactionReviewAuditRepository audit,
            OutboxRepository outbox,
            StripeChargeService stripe,
            ObjectMapper json,
            PlatformTransactionManager transactionManager) {
        this.transactions = transactions;
        this.audit = audit;
        this.outbox = outbox;
        this.stripe = stripe;
        this.json = json;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Transactional(readOnly = true)
    public List<Transaction> listHeld(String merchantId) {
        return transactions.findByMerchantIdAndStatusOrderByCreatedAtAsc(merchantId, TransactionStatus.HELD);
    }

    /**
     * Approves a held transaction and captures it.
     *
     * <p>Stripe is called between two short transactions rather than from inside one, for the same
     * reason as in {@link PaymentService}: an approval that charges the card and then fails to commit
     * would move money with no local record of it. On a Stripe failure the transaction is left HELD
     * and no audit row is written, so the reviewer can retry — {@code createCharge} uses the
     * transaction id as its idempotency key, so a retry after a partially-observed call does not
     * double-charge.
     */
    public Transaction approve(String transactionId, String merchantId, String paymentToken, String actorSub) {
        Transaction snapshot = transactionTemplate.execute(status -> heldTransaction(transactionId, merchantId));

        StripeChargeService.ChargeResult charge;
        try {
            charge = stripe.createCharge(snapshot, paymentToken);
        } catch (RuntimeException ex) {
            log.warn("Stripe charge failed while approving held transaction {}", transactionId, ex);
            throw ex;
        }

        return transactionTemplate.execute(status -> {
            Transaction tx = heldTransaction(transactionId, merchantId);
            tx.stripeChargeId(charge.chargeId());
            tx.status(TransactionStatus.COMPLETED);
            audit.save(new TransactionReviewAudit(transactionId, merchantId, "APPROVE", actorSub));
            publishCompleted(tx);
            return tx;
        });
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
            // Re-checked after the Stripe call as well as before it, so two reviewers approving the
            // same transaction concurrently cannot both write a COMPLETED row and an audit entry.
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
