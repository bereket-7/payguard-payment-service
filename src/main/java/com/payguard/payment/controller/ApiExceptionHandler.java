package com.payguard.payment.controller;

import com.payguard.payment.service.StripeChargeService;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps service-layer failures onto HTTP status codes.
 *
 * <p>Without this every one of these came back as a bare 500: a lookup for a payment belonging to
 * another merchant, a refund of a non-completed payment, and a card decline were indistinguishable
 * to the caller, and the decline in particular is a client-fixable condition being reported as a
 * server fault. Messages are deliberately terse — the exception text can name internal ids.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(NoSuchElementException.class)
    ResponseEntity<Map<String, Object>> notFound(NoSuchElementException ex, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "not_found", ex.getMessage(), request);
    }

    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<Map<String, Object>> forbidden(AccessDeniedException ex, HttpServletRequest request) {
        return problem(HttpStatus.FORBIDDEN, "forbidden", ex.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> invalid(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + " " + error.getDefaultMessage())
                .findFirst()
                .orElse("request validation failed");
        return problem(HttpStatus.BAD_REQUEST, "invalid_request", detail, request);
    }

    /** A declined card is the cardholder's problem, not an outage. */
    @ExceptionHandler(StripeChargeService.PaymentDeclinedException.class)
    ResponseEntity<Map<String, Object>> declined(
            StripeChargeService.PaymentDeclinedException ex, HttpServletRequest request) {
        return problem(HttpStatus.PAYMENT_REQUIRED, "card_declined", "the card was declined", request);
    }

    /** Stripe is rate-limiting or unreachable; the caller may retry. */
    @ExceptionHandler(StripeChargeService.RetryableStripeException.class)
    ResponseEntity<Map<String, Object>> upstreamUnavailable(
            StripeChargeService.RetryableStripeException ex, HttpServletRequest request) {
        log.warn("Stripe temporarily unavailable for {}", request.getRequestURI(), ex);
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE, "payment_provider_unavailable", "please retry shortly", request);
    }

    /** Lifecycle violations such as refunding a payment that never completed. */
    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Map<String, Object>> conflict(IllegalStateException ex, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "invalid_state", ex.getMessage(), request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "invalid_request", ex.getMessage(), request);
    }

    private ResponseEntity<Map<String, Object>> problem(
            HttpStatus status, String code, String detail, HttpServletRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("error", code);
        body.put("message", detail == null ? status.getReasonPhrase() : detail);
        body.put("path", request.getRequestURI());
        body.put("timestamp", Instant.now().toString());
        return ResponseEntity.status(status).body(body);
    }
}
