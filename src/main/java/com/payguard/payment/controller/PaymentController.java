package com.payguard.payment.controller;

import com.payguard.payment.domain.Transaction;
import com.payguard.payment.service.PaymentService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/payments")
public class PaymentController {

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    public record Create(
            @Min(1) long amountMinor,
            @NotBlank @Pattern(regexp = "[A-Za-z]{3}") String currency,
            String paymentToken) {
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(
            @Valid @RequestBody Create request, @AuthenticationPrincipal Jwt jwt) {
        Transaction tx = service.create(
                merchant(jwt),
                request.amountMinor(),
                request.currency().toUpperCase(),
                request.paymentToken());
        return ResponseEntity.status(HttpStatus.CREATED).body(view(tx));
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable String id, @AuthenticationPrincipal Jwt jwt) {
        return view(service.get(id, merchant(jwt)));
    }

    @PostMapping("/{id}/refund")
    public Map<String, Object> refund(@PathVariable String id, @AuthenticationPrincipal Jwt jwt) {
        return view(service.refund(id, merchant(jwt)));
    }

    private String merchant(Jwt jwt) {
        String id = jwt.getClaimAsString("merchant_id");
        if (id == null) {
            throw new org.springframework.security.access.AccessDeniedException("merchant_id claim is required");
        }
        return id;
    }

    static Map<String, Object> view(Transaction t) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("transaction_id", t.getTransactionId());
        v.put("status", t.getStatus());
        v.put("fraud_score", t.getFraudScore());
        v.put("fraud_decision", t.getFraudDecision());
        v.put("stripe_charge_id", t.getStripeChargeId());
        return v;
    }
}
