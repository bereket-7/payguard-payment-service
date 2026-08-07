package com.payguard.payment.repository;

import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    Optional<Transaction> findByTransactionId(String transactionId);

    Optional<Transaction> findByTransactionIdAndMerchantId(String id, String merchant);

    Optional<Transaction> findByStripeChargeId(String id);

    List<Transaction> findByMerchantIdAndStatusOrderByCreatedAtAsc(String merchant, TransactionStatus status);
}
