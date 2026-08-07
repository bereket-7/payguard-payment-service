package com.payguard.payment.service;

import com.payguard.payment.domain.Transaction;
import com.stripe.exception.ApiConnectionException;
import com.stripe.exception.CardException;
import com.stripe.exception.RateLimitException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.RefundCreateParams;
import org.springframework.stereotype.Service;

@Service
public class StripeChargeService {

    public record ChargeResult(String chargeId) {
    }

    public ChargeResult createCharge(Transaction transaction, String paymentToken) {
        try {
            PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                    .setAmount(transaction.getAmountMinor())
                    .setCurrency(transaction.getCurrency().toLowerCase())
                    .setPaymentMethod(paymentToken)
                    .setConfirm(true)
                    .setAutomaticPaymentMethods(PaymentIntentCreateParams.AutomaticPaymentMethods.builder()
                            .setEnabled(true)
                            .setAllowRedirects(PaymentIntentCreateParams.AutomaticPaymentMethods.AllowRedirects.NEVER)
                            .build())
                    .putMetadata("transaction_id", transaction.getTransactionId())
                    .putMetadata("merchant_id", transaction.getMerchantId())
                    .build();

            RequestOptions options = RequestOptions.builder()
                    .setIdempotencyKey(transaction.getTransactionId())
                    .build();

            PaymentIntent intent = PaymentIntent.create(params, options);
            String chargeId = intent.getLatestCharge();
            if (chargeId == null || chargeId.isBlank()) {
                chargeId = intent.getId();
            }
            return new ChargeResult(chargeId);
        } catch (CardException ex) {
            throw new PaymentDeclinedException("card declined", ex);
        } catch (RateLimitException ex) {
            throw new RetryableStripeException("stripe rate limit", ex);
        } catch (ApiConnectionException ex) {
            throw new RetryableStripeException("stripe connection error", ex);
        } catch (StripeException ex) {
            throw new IllegalStateException("stripe charge failed", ex);
        }
    }

    public void refund(Transaction transaction) {
        if (transaction.getStripeChargeId() == null) {
            throw new IllegalStateException("transaction has no stripe charge");
        }
        try {
            RefundCreateParams params = RefundCreateParams.builder()
                    .setCharge(transaction.getStripeChargeId())
                    .build();
            RequestOptions options = RequestOptions.builder()
                    .setIdempotencyKey("refund-" + transaction.getTransactionId())
                    .build();
            Refund.create(params, options);
        } catch (RateLimitException | ApiConnectionException ex) {
            throw new RetryableStripeException("stripe refund retryable failure", ex);
        } catch (StripeException ex) {
            throw new IllegalStateException("stripe refund failed", ex);
        }
    }

    public static class PaymentDeclinedException extends RuntimeException {
        public PaymentDeclinedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class RetryableStripeException extends RuntimeException {
        public RetryableStripeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
