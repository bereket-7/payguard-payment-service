package com.payguard.payment.outbox;

import com.payguard.payment.kafka.JsonAvroConverter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final JsonAvroConverter jsonAvroConverter;
    private final int batchSize;

    public OutboxRelay(
            OutboxRepository repository,
            KafkaTemplate<String, SpecificRecord> kafkaTemplate,
            JsonAvroConverter jsonAvroConverter,
            @Value("${payguard.outbox.batch-size:50}") int batchSize) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.jsonAvroConverter = jsonAvroConverter;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${payguard.outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEntry> pending = repository.lockUnpublished(batchSize);
        for (OutboxEntry entry : pending) {
            try {
                SpecificRecord record = jsonAvroConverter.toRecord(entry.getTopic(), entry.getPayload());
                kafkaTemplate.send(entry.getTopic(), entry.getPartitionKey(), record).get();
                entry.markPublished();
                log.debug("Published outbox event {} to topic {}", entry.getEventId(), entry.getTopic());
            } catch (Exception ex) {
                log.warn("Failed to publish outbox event {} to topic {}", entry.getEventId(), entry.getTopic(), ex);
                throw new IllegalStateException("outbox publish failed", ex);
            }
        }
    }
}
