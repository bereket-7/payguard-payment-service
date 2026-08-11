package com.payguard.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class OutboxEntryTest {

    @Test
    void schedulesRetryWithBackoffOnFailure() {
        OutboxEntry entry = new OutboxEntry(
                "evt_1", "txn_1", "payment.created", "payment.created", "mer_1", "{}");
        Instant before = Instant.now();

        entry.recordFailure("broker unavailable", Duration.ofSeconds(2));

        assertThat(entry.getAttempts()).isEqualTo(1);
        assertThat(entry.getLastError()).contains("broker unavailable");
        assertThat(entry.getNextAttemptAt()).isAfter(before);
    }

    @Test
    void truncatesVeryLongErrors() {
        OutboxEntry entry = new OutboxEntry(
                "evt_2", "txn_2", "payment.failed", "payment.failed", "mer_1", "{}");
        String longError = "x".repeat(5_000);

        entry.recordFailure(longError, Duration.ofSeconds(1));

        assertThat(entry.getLastError()).hasSize(2_000);
    }

    @Test
    void marksDeadLetteredWithoutClearingPublishedTimestamp() {
        OutboxEntry entry = new OutboxEntry(
                "evt_3", "txn_3", "payment.created", "payment.created", "mer_1", "{}");

        entry.markDeadLettered("exhausted retries");

        assertThat(entry.getDeadLetteredAt()).isNotNull();
        assertThat(entry.getPublishedAt()).isNull();
        assertThat(entry.getLastError()).contains("exhausted retries");
    }
}
