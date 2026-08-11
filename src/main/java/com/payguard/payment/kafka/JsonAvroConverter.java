package com.payguard.payment.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.events.payment.PaymentCompleted;
import com.payguard.events.payment.PaymentCreated;
import com.payguard.events.payment.PaymentFailed;
import com.payguard.events.payment.PaymentHeld;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
                    case "payment.held" -> new PaymentHeld();
                    default -> throw new IllegalArgumentException("unsupported outbox topic: " + topic);
                };
        populate(record, node);
        return record;
    }

    private void populate(SpecificRecord record, JsonNode node) {
        for (Schema.Field field : record.getSchema().getFields()) {
            JsonNode value = node.get(field.name());

            if (value == null || value.isNull()) {
                // Silently skipping a required field used to defer the failure to the Kafka serializer,
                // which reports it as an opaque NullPointerException with no field name attached.
                if (!isNullable(field.schema())) {
                    throw new IllegalArgumentException(
                            "outbox payload for " + record.getSchema().getName()
                                    + " is missing required field '" + field.name() + "'");
                }
                continue;
            }

            // Positional put: the declared type here is SpecificRecord, whose only put() is the
            // IndexedRecord put(int, Object) — the name-based overload lives on SpecificRecordBase.
            record.put(field.pos(), convertValue(value, field.schema(), field.name()));
        }
    }

    /**
     * Converts one JSON node to the Avro representation its field schema demands.
     *
     * <p>Unions are resolved to their single non-null branch first. The previous version switched
     * only on the top-level type, so a field declared {@code ["null","long"]} fell through to the
     * default branch and was written as a {@code String} — which the Avro serializer then rejected
     * at send time, or worse, silently mis-typed downstream consumers.
     */
    private Object convertValue(JsonNode value, Schema schema, String fieldName) {
        Schema resolved = resolveUnion(schema, fieldName);
        return switch (resolved.getType()) {
            case STRING -> value.asText();
            case LONG -> asLong(value, fieldName);
            case INT -> asInt(value, fieldName);
            case DOUBLE -> asDouble(value, fieldName);
            case FLOAT -> (float) asDouble(value, fieldName);
            case BOOLEAN -> value.asBoolean();
            case ENUM -> new GenericData.EnumSymbol(resolved, value.asText());
            case BYTES -> ByteBuffer.wrap(decodeBytes(value, fieldName));
            case FIXED -> new GenericData.Fixed(resolved, decodeBytes(value, fieldName));
            case ARRAY -> convertArray(value, resolved, fieldName);
            case MAP -> convertMap(value, resolved, fieldName);
            case RECORD -> convertRecord(value, resolved, fieldName);
            case NULL -> null;
        };
    }

    /** Returns the sole non-null branch of a union, or the schema itself when it is not a union. */
    private Schema resolveUnion(Schema schema, String fieldName) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        Schema branch = null;
        for (Schema candidate : schema.getTypes()) {
            if (candidate.getType() == Schema.Type.NULL) {
                continue;
            }
            if (branch != null) {
                // Picking a branch by guesswork would silently produce the wrong wire type; the
                // outbox payload carries no discriminator to choose with.
                throw new IllegalArgumentException(
                        "field '" + fieldName + "' uses a multi-branch union " + schema
                                + " which the outbox JSON cannot disambiguate");
            }
            branch = candidate;
        }
        return branch == null ? Schema.create(Schema.Type.NULL) : branch;
    }

    private boolean isNullable(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        if (schema.getType() != Schema.Type.UNION) {
            return false;
        }
        return schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private List<Object> convertArray(JsonNode value, Schema schema, String fieldName) {
        if (!value.isArray()) {
            throw new IllegalArgumentException("field '" + fieldName + "' expects a JSON array");
        }
        List<Object> items = new ArrayList<>(value.size());
        value.forEach(element -> items.add(convertValue(element, schema.getElementType(), fieldName + "[]")));
        return items;
    }

    private Map<String, Object> convertMap(JsonNode value, Schema schema, String fieldName) {
        if (!value.isObject()) {
            throw new IllegalArgumentException("field '" + fieldName + "' expects a JSON object");
        }
        Map<String, Object> entries = new LinkedHashMap<>();
        value.fields()
                .forEachRemaining(entry -> entries.put(
                        entry.getKey(),
                        convertValue(entry.getValue(), schema.getValueType(), fieldName + "." + entry.getKey())));
        return entries;
    }

    private GenericData.Record convertRecord(JsonNode value, Schema schema, String fieldName) {
        if (!value.isObject()) {
            throw new IllegalArgumentException("field '" + fieldName + "' expects a JSON object");
        }
        GenericData.Record nested = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            JsonNode child = value.get(field.name());
            if (child == null || child.isNull()) {
                if (!isNullable(field.schema())) {
                    throw new IllegalArgumentException(
                            "nested record '" + fieldName + "' is missing required field '" + field.name() + "'");
                }
                continue;
            }
            nested.put(field.pos(), convertValue(child, field.schema(), fieldName + "." + field.name()));
        }
        return nested;
    }

    private long asLong(JsonNode value, String fieldName) {
        if (!value.canConvertToLong()) {
            throw new IllegalArgumentException("field '" + fieldName + "' is not a long: " + value);
        }
        return value.asLong();
    }

    private int asInt(JsonNode value, String fieldName) {
        if (!value.canConvertToInt()) {
            throw new IllegalArgumentException("field '" + fieldName + "' is not an int: " + value);
        }
        return value.asInt();
    }

    private double asDouble(JsonNode value, String fieldName) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException("field '" + fieldName + "' is not a number: " + value);
        }
        return value.asDouble();
    }

    private byte[] decodeBytes(JsonNode value, String fieldName) {
        try {
            return value.isBinary() ? value.binaryValue() : Base64.getDecoder().decode(value.asText());
        } catch (IOException | IllegalArgumentException ex) {
            throw new IllegalArgumentException("field '" + fieldName + "' is not base64-encoded bytes", ex);
        }
    }
}
