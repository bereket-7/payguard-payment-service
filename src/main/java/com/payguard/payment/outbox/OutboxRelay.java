package com.payguard.payment.outbox;

import com.payguard.payment.kafka.JsonAvroConverter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    /** Suffix convention shared with the notification service's DeadLetterPublishingRecoverer. */
    private static final String DLQ_SUFFIX = ".dlq";

    private static final Duration MAX_BACKOFF = Duration.ofMinutes(10);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final KafkaTemplate<String, String> dlqKafkaTemplate;
    private final JsonAvroConverter jsonAvroConverter;
    private final int batchSize;
    private final int maxAttempts;
    private final Duration baseBackoff;
    private final long sendTimeoutMs;
    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter deadLetteredCounter;

    public OutboxRelay(
            OutboxRepository repository,
            KafkaTemplate<String, SpecificRecord> kafkaTemplate,
            KafkaTemplate<String, String> dlqKafkaTemplate,
            JsonAvroConverter jsonAvroConverter,
            MeterRegistry meterRegistry,
            @Value("${payguard.outbox.batch-size:50}") int batchSize,
            @Value("${payguard.outbox.max-attempts:8}") int maxAttempts,
            @Value("${payguard.outbox.base-backoff-ms:1000}") long baseBackoffMs,
            @Value("${payguard.outbox.send-timeout-ms:10000}") long sendTimeoutMs) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.dlqKafkaTemplate = dlqKafkaTemplate;
        this.jsonAvroConverter = jsonAvroConverter;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.baseBackoff = Duration.ofMillis(baseBackoffMs);
        this.sendTimeoutMs = sendTimeoutMs;
        this.publishedCounter = Counter.builder("payguard.outbox.published")
                .description("Outbox entries published to Kafka")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("payguard.outbox.publish.failed")
                .description("Outbox publish attempts that failed and will be retried")
                .register(meterRegistry);
        this.deadLetteredCounter = Counter.builder("payguard.outbox.dead.lettered")
                .description("Outbox entries that exhausted their retries and were dead-lettered")
                .register(meterRegistry);
        meterRegistry.gauge(
                "payguard.outbox.backlog", repository, OutboxRepository::countByPublishedAtIsNullAndDeadLetteredAtIsNull);
        meterRegistry.gauge("payguard.outbox.dead.letter.depth", repository, OutboxRepository::countByDeadLetteredAtIsNotNull);
    }

    /**
     * Publishes one batch of pending events.
     *
     * <p>Each entry is isolated: a failure updates only that entry's retry state and the loop
     * continues. The previous implementation rethrew, which rolled the batch's transaction back —
     * losing the {@code markPublished()} of every entry that had already succeeded (re-publishing
     * them on the next poll) and leaving the poison entry first in line again one second later.
     *
     * <p>Kafka send failures do not poison the JPA transaction, so recording retry state on the
     * failing row inside this same transaction is safe.
     */
    @Scheduled(fixedDelayString = "${payguard.outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEntry> batch = repository.lockPublishable(Instant.now(), batchSize);
        for (OutboxEntry entry : batch) {
            try {
                SpecificRecord record = jsonAvroConverter.toRecord(entry.getTopic(), entry.getPayload());
                kafkaTemplate
                        .send(entry.getTopic(), entry.getPartitionKey(), record)
                        .get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                entry.markPublished();
                publishedCounter.increment();
                log.debug("Published outbox event {} to topic {}", entry.getEventId(), entry.getTopic());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                handleFailure(entry, ex);
                return;
            } catch (Exception ex) {
                handleFailure(entry, ex);
            }
        }
    }

    private void handleFailure(OutboxEntry entry, Exception ex) {
        String reason = ex.getClass().getSimpleName() + ": " + ex.getMessage();

        if (entry.getAttempts() + 1 >= maxAttempts) {
            deadLetter(entry, reason, ex);
            return;
        }

        Duration backoff = backoffFor(entry.getAttempts());
        entry.recordFailure(reason, backoff);
        failedCounter.increment();
        log.warn(
                "Failed to publish outbox event {} to topic {} (attempt {}/{}), retrying in {}",
                entry.getEventId(),
                entry.getTopic(),
                entry.getAttempts(),
                maxAttempts,
                backoff,
                ex);
    }

    /**
     * Copies an exhausted entry to {@code <topic>.dlq} as its original JSON so it can be replayed
     * through the notification service's DLQ admin endpoint, then marks it so the relay moves on.
     *
     * <p>The DLQ producer is String-valued on purpose: the most common reason an entry gets here is
     * that the payload cannot be turned into an Avro record at all, so the Avro template could not
     * carry it.
     */
    private void deadLetter(OutboxEntry entry, String reason, Exception cause) {
        String dlqTopic = entry.getTopic() + DLQ_SUFFIX;
        try {
            dlqKafkaTemplate
                    .send(dlqTopic, entry.getPartitionKey(), entry.getPayload())
                    .get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Leave the entry retryable rather than dropping it — the payload is still only in the DB.
            entry.recordFailure("dead-letter publish interrupted: " + reason, backoffFor(entry.getAttempts()));
            failedCounter.increment();
            return;
        } catch (Exception ex) {
            entry.recordFailure("dead-letter publish failed: " + reason, backoffFor(entry.getAttempts()));
            failedCounter.increment();
            log.error(
                    "Could not dead-letter outbox event {} to {}; keeping it queued for retry",
                    entry.getEventId(),
                    dlqTopic,
                    ex);
            return;
        }

        entry.markDeadLettered(reason);
        deadLetteredCounter.increment();
        log.error(
                "Outbox event {} exhausted {} attempts and was dead-lettered to {}: {}",
                entry.getEventId(),
                maxAttempts,
                dlqTopic,
                reason,
                cause);
    }

    private Duration backoffFor(int attempts) {
        // 1s, 2s, 4s, ... capped, so a broken schema registry does not become a hot loop.
        long multiplier = 1L << Math.min(attempts, 20);
        Duration backoff = baseBackoff.multipliedBy(multiplier);
        return backoff.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff;
    }
}
