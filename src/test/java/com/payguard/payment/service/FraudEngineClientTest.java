package com.payguard.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

class FraudEngineClientTest {

    private FraudEngineClient client;

    @BeforeEach
    void setUp() {
        client = new FraudEngineClient(
                RestClient.builder(),
                CircuitBreakerRegistry.ofDefaults(),
                "http://127.0.0.1:1",
                50,
                10_000,
                "");
    }

    @Test
    void fallsBackToApproveForSmallAmountsWhenEngineUnreachable() {
        FraudEngineClient.Decision decision = client.score("txn_1", "mer_1", 5_000, "USD");

        assertThat(decision.decision()).isEqualTo("APPROVE");
        assertThat(decision.score()).isEqualTo(-1);
    }

    @Test
    void fallsBackToReviewForLargeAmountsWhenEngineUnreachable() {
        FraudEngineClient.Decision decision = client.score("txn_2", "mer_1", 50_000, "USD");

        assertThat(decision.decision()).isEqualTo("REVIEW");
        assertThat(decision.score()).isEqualTo(-1);
    }
}
