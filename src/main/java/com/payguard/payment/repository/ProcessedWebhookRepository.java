package com.payguard.payment.repository;

import com.payguard.payment.domain.ProcessedWebhook;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedWebhookRepository extends JpaRepository<ProcessedWebhook, UUID> {
    boolean existsByStripeEventId(String stripeEventId);
}
