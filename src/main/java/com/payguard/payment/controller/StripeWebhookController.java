package com.payguard.payment.controller;

import com.payguard.payment.service.WebhookProcessingService;
import com.stripe.exception.SignatureVerificationException;
import com.stripe.model.Event;
import com.stripe.net.Webhook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/webhooks/stripe")
public class StripeWebhookController {

    private static final Logger log = LoggerFactory.getLogger(StripeWebhookController.class);

    private final WebhookProcessingService service;
    private final String webhookSecret;

    public StripeWebhookController(
            WebhookProcessingService service,
            @Value("${payguard.stripe.webhook-secret:}") String webhookSecret) {
        this.service = service;
        this.webhookSecret = webhookSecret;
    }

    @PostMapping
    public ResponseEntity<String> handle(
            @RequestBody String payload, @RequestHeader("Stripe-Signature") String signature) {
        if (webhookSecret == null || webhookSecret.isBlank()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("webhook secret not configured");
        }

        Event event;
        try {
            event = Webhook.constructEvent(payload, signature, webhookSecret);
        } catch (SignatureVerificationException ex) {
            return ResponseEntity.badRequest().body("invalid signature");
        }

        try {
            service.process(event);
        } catch (WebhookProcessingService.DuplicateWebhookException ex) {
            // A concurrent delivery of the same event won the race to claim it and is applying it
            // now. Answering 200 stops Stripe retrying a delivery that is already being handled.
            log.debug("Duplicate delivery of Stripe event {}", event.getId());
            return ResponseEntity.ok("duplicate");
        }
        return ResponseEntity.ok("ok");
    }
}
