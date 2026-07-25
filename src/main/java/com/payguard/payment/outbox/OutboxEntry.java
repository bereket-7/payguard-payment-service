package com.payguard.payment.outbox;
/** A durable event record written with the payment aggregate transaction. */
public record OutboxEntry(String eventId, String aggregateId, String eventType, String payload) {}
