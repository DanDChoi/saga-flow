package com.dan.sagaflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.order.domain.Order;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

class OrderEventPublisherTest {

    @SuppressWarnings("unchecked")
    KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate = mock(KafkaTemplate.class);

    OrderEventPublisher publisher = new OrderEventPublisher(kafkaTemplate);

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void 주문_생성_이벤트를_order_events_토픽에_주문ID를_키로_발행한다() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(EventEnvelope.class)))
                .thenReturn(new CompletableFuture<>());
        Order order = Order.create(1L, 100L, 3, 15000L);
        ReflectionTestUtils.setField(order, "id", 7L);

        publisher.publishOrderCreated(order);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.ORDER_EVENTS), eq("7"), captor.capture());
        EventEnvelope<?> event = captor.getValue();
        assertThat(event.eventType()).isEqualTo("ORDER_CREATED");
        assertThat(event.aggregateId()).isEqualTo("7");
        assertThat(event.payload()).isEqualTo(new OrderCreatedPayload(7L, 1L, 100L, 3, 15000L));
    }
}
