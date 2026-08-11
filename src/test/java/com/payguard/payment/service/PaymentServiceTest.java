package com.payguard.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payguard.payment.domain.Transaction;
import com.payguard.payment.domain.TransactionStatus;
import com.payguard.payment.outbox.OutboxEntry;
import com.payguard.payment.outbox.OutboxRepository;
import com.payguard.payment.repository.TransactionRepository;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.ResourcelessTransactionManager;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private TransactionRepository transactions;

    @Mock
    private OutboxRepository outbox;

    @Mock
    private FraudEngineClient fraud;

    @Mock
    private StripeChargeService stripe;

    private PaymentService service;

    @BeforeEach
    void setUp() {
        service = new PaymentService(
                transactions,
                outbox,
                fraud,
                stripe,
                new ObjectMapper(),
                new ResourcelessTransactionManager());
    }

    @Test
    void approvesPaymentWhenFraudApprovesWithoutPaymentToken() {
        when(fraud.score(anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(new FraudEngineClient.Decision(12.0, "APPROVE"));
        when(transactions.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(transactions.findByTransactionId(anyString())).thenAnswer(invocation -> {
            Transaction t = new Transaction("ignored", "mer_1", 5_000, "USD");
            return Optional.of(t);
        });

        Transaction result = service.create("mer_1", 5_000, "USD", "");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.APPROVED);
        verify(stripe, never()).createCharge(any(), anyString());
        verify(outbox, org.mockito.Mockito.atLeastOnce()).save(any(OutboxEntry.class));
    }

    @Test
    void blocksPaymentWhenFraudBlocks() {
        when(fraud.score(anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(new FraudEngineClient.Decision(95.0, "BLOCK"));
        when(transactions.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(transactions.findByTransactionId(anyString())).thenAnswer(invocation -> {
            Transaction t = new Transaction("ignored", "mer_1", 5_000, "USD");
            return Optional.of(t);
        });

        Transaction result = service.create("mer_1", 5_000, "USD", "pm_test");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.BLOCKED);
        verify(stripe, never()).createCharge(any(), anyString());
    }

    @Test
    void holdsPaymentForManualReview() {
        when(fraud.score(anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(new FraudEngineClient.Decision(55.0, "REVIEW"));
        when(transactions.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(transactions.findByTransactionId(anyString())).thenAnswer(invocation -> {
            Transaction t = new Transaction("ignored", "mer_1", 5_000, "USD");
            return Optional.of(t);
        });

        Transaction result = service.create("mer_1", 5_000, "USD", "pm_test");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.HELD);

        ArgumentCaptor<OutboxEntry> captor = ArgumentCaptor.forClass(OutboxEntry.class);
        verify(outbox, org.mockito.Mockito.atLeast(2)).save(captor.capture());
        assertThat(captor.getAllValues())
                .anyMatch(entry -> "payment.held".equals(entry.getEventType()));
    }

    @Test
    void completesPaymentWhenFraudApprovesAndStripeSucceeds() {
        when(fraud.score(anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(new FraudEngineClient.Decision(5.0, "APPROVE"));
        when(transactions.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(transactions.findByTransactionId(anyString())).thenAnswer(invocation -> {
            Transaction t = new Transaction("ignored", "mer_1", 5_000, "USD");
            return Optional.of(t);
        });
        when(stripe.createCharge(any(Transaction.class), anyString()))
                .thenReturn(new StripeChargeService.ChargeResult("ch_123"));

        Transaction result = service.create("mer_1", 5_000, "USD", "pm_test");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(result.getStripeChargeId()).isEqualTo("ch_123");
    }

    @Test
    void marksFailedWhenStripeDeclinesAfterFraudApproval() {
        when(fraud.score(anyString(), anyString(), anyLong(), anyString()))
                .thenReturn(new FraudEngineClient.Decision(5.0, "APPROVE"));
        when(transactions.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(transactions.findByTransactionId(anyString())).thenAnswer(invocation -> {
            Transaction t = new Transaction("ignored", "mer_1", 5_000, "USD");
            return Optional.of(t);
        });
        when(stripe.createCharge(any(Transaction.class), anyString()))
                .thenThrow(new StripeChargeService.PaymentDeclinedException("card declined", new RuntimeException("stripe")));

        Transaction result = service.create("mer_1", 5_000, "USD", "pm_test");

        assertThat(result.getStatus()).isEqualTo(TransactionStatus.FAILED);
    }
}
