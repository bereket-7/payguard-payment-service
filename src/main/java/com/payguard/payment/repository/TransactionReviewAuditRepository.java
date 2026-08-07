package com.payguard.payment.repository;

import com.payguard.payment.domain.TransactionReviewAudit;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionReviewAuditRepository extends JpaRepository<TransactionReviewAudit, UUID> {
}
