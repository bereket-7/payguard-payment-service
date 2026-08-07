package com.payguard.payment.config;

import com.payguard.payment.kafka.JsonAvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

    @Bean
    @Primary
    ProducerFactory<String, SpecificRecord> avroProducerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url:http://localhost:8081}") String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    @Primary
    KafkaTemplate<String, SpecificRecord> avroKafkaTemplate(
            ProducerFactory<String, SpecificRecord> avroProducerFactory) {
        return new KafkaTemplate<>(avroProducerFactory);
    }

    @Bean
    JsonAvroConverter jsonAvroConverter(com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        return new JsonAvroConverter(objectMapper);
    }
}
