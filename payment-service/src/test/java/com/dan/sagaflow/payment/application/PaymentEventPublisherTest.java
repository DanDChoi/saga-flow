package com.dan.sagaflow.payment.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.PaymentCompletedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.payment.domain.Payment;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
class PaymentEventPublisherTest {

    KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate = mock(KafkaTemplate.class);
    PaymentEventPublisher publisher = new PaymentEventPublisher(kafkaTemplate);

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(EventEnvelope.class)))
                .thenReturn(new CompletableFuture<>());
    }

    @Test
    void PAYMENT_COMPLETED를_payment_events_토픽에_주문ID를_키로_발행한다() {
        Payment payment = Payment.completed(7L, 15_000L);
        ReflectionTestUtils.setField(payment, "id", 3L);

        publisher.publishPaymentCompleted(payment);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.PAYMENT_EVENTS), eq("7"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(captor.getValue().payload()).isEqualTo(new PaymentCompletedPayload(7L, 3L, 15_000L));
    }

    @Test
    void PAYMENT_FAILED를_사유와_함께_발행한다() {
        publisher.publishPaymentFailed(Payment.failed(7L, 5_000_000L, "결제 한도 초과"));

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.PAYMENT_EVENTS), eq("7"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo("PAYMENT_FAILED");
        assertThat(captor.getValue().payload()).isEqualTo(new PaymentFailedPayload(7L, "결제 한도 초과"));
    }
}
