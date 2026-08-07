package com.payguard.payment.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.events.payment.PaymentCompleted;
import com.payguard.events.payment.PaymentCreated;
import com.payguard.events.payment.PaymentFailed;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

@Component
public class JsonAvroConverter {

    private final ObjectMapper objectMapper;

    public JsonAvroConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SpecificRecord toRecord(String topic, String json) throws IOException {
        JsonNode node = objectMapper.readTree(json);
        SpecificRecord record =
                switch (topic) {
                    case "payment.created" -> new PaymentCreated();
                    case "payment.completed" -> new PaymentCompleted();
                    case "payment.failed" -> new PaymentFailed();
                    default -> throw new IllegalArgumentException("unsupported outbox topic: " + topic);
                };
        for (Schema.Field field : record.getSchema().getFields()) {
            JsonNode value = node.get(field.name());
            if (value == null || value.isNull()) {
                continue;
            }
            record.put(field.name(), convertValue(value, field.schema()));
        }
        return record;
    }

    private Object convertValue(JsonNode value, Schema schema) {
        return switch (schema.getType()) {
            case LONG -> value.asLong();
            case INT -> value.asInt();
            case DOUBLE, FLOAT -> value.asDouble();
            case BOOLEAN -> value.asBoolean();
            default -> value.asText();
        };
    }
}
