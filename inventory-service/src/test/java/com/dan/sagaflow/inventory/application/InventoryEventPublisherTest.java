package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.StockFailedPayload;
import com.dan.sagaflow.common.event.StockReleasedPayload;
import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

@SuppressWarnings({"unchecked", "rawtypes"})
class InventoryEventPublisherTest {

    KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate = mock(KafkaTemplate.class);
    InventoryEventPublisher publisher = new InventoryEventPublisher(kafkaTemplate);
    OrderCreatedPayload order = new OrderCreatedPayload(7L, 1L, 100L, 3, 15000L);

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(EventEnvelope.class)))
                .thenReturn(new CompletableFuture<>());
    }

    @Test
    void STOCK_RESERVED를_inventory_events_토픽에_주문ID를_키로_발행한다() {
        publisher.publishStockReserved(order);

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.INVENTORY_EVENTS), eq("7"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo("STOCK_RESERVED");
        assertThat(captor.getValue().payload()).isEqualTo(new StockReservedPayload(7L, 100L, 3, 15000L));
    }

    @Test
    void STOCK_RELEASED를_발행한다() {
        publisher.publishStockReleased(new PaymentFailedPayload(7L, 100L, 3, "결제 한도 초과"));

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.INVENTORY_EVENTS), eq("7"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo("STOCK_RELEASED");
        assertThat(captor.getValue().payload()).isEqualTo(new StockReleasedPayload(7L, 100L, 3));
    }

    @Test
    void STOCK_FAILED를_사유와_함께_발행한다() {
        publisher.publishStockFailed(order, "재고 부족");

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(Topics.INVENTORY_EVENTS), eq("7"), captor.capture());
        assertThat(captor.getValue().eventType()).isEqualTo("STOCK_FAILED");
        assertThat(captor.getValue().payload()).isEqualTo(new StockFailedPayload(7L, 100L, "재고 부족"));
    }
}
