package com.payguard.payment.controller;

import com.payguard.payment.domain.Transaction;
import com.payguard.payment.service.TransactionReviewService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/transactions")
public class TransactionReviewController {

    private final TransactionReviewService reviews;

    public TransactionReviewController(TransactionReviewService reviews) {
        this.reviews = reviews;
    }

    public record ApproveRequest(@NotBlank String paymentToken) {
    }

    @GetMapping
    public List<Map<String, Object>> listHeld(
            @RequestParam(defaultValue = "HELD") String status, @AuthenticationPrincipal Jwt jwt) {
        if (!"HELD".equalsIgnoreCase(status)) {
            throw new IllegalArgumentException("only HELD status is supported");
        }
        return reviews.listHeld(merchant(jwt)).stream()
                .map(PaymentController::view)
                .collect(Collectors.toList());
    }

    @PostMapping("/{id}/approve")
    @PreAuthorize("hasAuthority('payment:review')")
    public Map<String, Object> approve(
            @PathVariable String id,
            @Valid @RequestBody ApproveRequest request,
            @AuthenticationPrincipal Jwt jwt) {
        Transaction tx = reviews.approve(id, merchant(jwt), request.paymentToken(), jwt.getSubject());
        return PaymentController.view(tx);
    }

    @PostMapping("/{id}/reject")
    @PreAuthorize("hasAuthority('payment:review')")
    public Map<String, Object> reject(@PathVariable String id, @AuthenticationPrincipal Jwt jwt) {
        Transaction tx = reviews.reject(id, merchant(jwt), jwt.getSubject());
        return PaymentController.view(tx);
    }

    private String merchant(Jwt jwt) {
        String id = jwt.getClaimAsString("merchant_id");
        if (id == null) {
            throw new org.springframework.security.access.AccessDeniedException("merchant_id claim is required");
        }
        return id;
    }
}
