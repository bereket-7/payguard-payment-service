package com.payguard.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transaction_review_audit")
public class TransactionReviewAudit {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, updatable = false)
    private String transactionId;

    @Column(name = "merchant_id", nullable = false, updatable = false)
    private String merchantId;

    @Column(nullable = false, updatable = false)
    private String action;

    @Column(name = "actor_sub", nullable = false, updatable = false)
    private String actorSub;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected TransactionReviewAudit() {
    }

    public TransactionReviewAudit(String transactionId, String merchantId, String action, String actorSub) {
        this.transactionId = transactionId;
        this.merchantId = merchantId;
        this.action = action;
        this.actorSub = actorSub;
    }

    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
    }
}
