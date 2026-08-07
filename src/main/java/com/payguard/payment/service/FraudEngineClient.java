package com.payguard.payment.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

@Component
public class FraudEngineClient {

    public static final String INTERNAL_TOKEN_HEADER = "X-PayGuard-Internal-Token";

    public record Decision(double score, String decision) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ScoreResponse(
            @JsonProperty("transaction_id") String transactionId,
            double score,
            String decision) {
    }

    private final RestClient client;
    private final CircuitBreaker breaker;
    private final long threshold;
    private final String internalServiceToken;

    public FraudEngineClient(
            RestClient.Builder builder,
            CircuitBreakerRegistry registry,
            @Value("${payguard.fraud.base-url}") String url,
            @Value("${payguard.fraud.timeout-ms}") long timeout,
            @Value("${payguard.fraud.fallback-approve-max-amount-minor}") long threshold,
            @Value("${payguard.internal.service-token:}") String internalServiceToken) {
        client = builder.baseUrl(url)
                .requestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory() {
                    {
                        setConnectTimeout(Duration.ofMillis(timeout));
                        setReadTimeout(Duration.ofMillis(timeout));
                    }
                })
                .build();
        breaker = registry.circuitBreaker("fraudEngine");
        this.threshold = threshold;
        this.internalServiceToken = internalServiceToken;
    }

    public Decision score(String id, String merchant, long amount, String currency) {
        try {
            return CircuitBreaker.decorateSupplier(breaker, () -> {
                RestClient.RequestBodySpec request = client.post().uri("/internal/v1/score");
                if (StringUtils.hasText(internalServiceToken)) {
                    request = request.header(INTERNAL_TOKEN_HEADER, internalServiceToken);
                }
                ScoreResponse response = request.body(Map.of(
                                "transaction_id", id,
                                "merchant_id", merchant,
                                "amount_minor", amount,
                                "currency", currency))
                        .retrieve()
                        .body(ScoreResponse.class);
                return new Decision(response.score(), response.decision());
            }).get();
        } catch (Exception ignored) {
            return new Decision(-1, amount <= threshold ? "APPROVE" : "REVIEW");
        }
    }
}
