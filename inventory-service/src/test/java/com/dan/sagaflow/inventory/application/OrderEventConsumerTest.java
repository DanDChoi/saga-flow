package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.inventory.domain.InsufficientStockException;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class OrderEventConsumerTest {

    static final String MESSAGE = """
            {"eventId":"e-1","eventType":"ORDER_CREATED","aggregateId":"7",
             "occurredAt":"2026-11-03T00:00:00Z",
             "payload":{"orderId":7,"customerId":1,"productId":100,"quantity":3,"totalAmount":15000}}
            """;

    StockService stockService = mock(StockService.class);
    InventoryEventPublisher eventPublisher = mock(InventoryEventPublisher.class);
    OrderEventConsumer consumer = new OrderEventConsumer(JsonMapper.builder().build(), stockService, eventPublisher);
    OrderCreatedPayload order = new OrderCreatedPayload(7L, 1L, 100L, 3, 15000L);

    @Test
    void ORDER_CREATED_메시지를_받으면_상품과_수량으로_재고를_차감한다() {
        consumer.onOrderEvent(MESSAGE);

        verify(stockService).decrease(100L, 3);
    }

    @Test
    void 재고_차감에_성공하면_STOCK_RESERVED를_발행한다() {
        consumer.onOrderEvent(MESSAGE);

        verify(eventPublisher).publishStockReserved(order);
        verify(eventPublisher, never()).publishStockFailed(any(), anyString());
    }

    @Test
    void 재고가_부족하면_STOCK_FAILED를_발행한다() {
        doThrow(new InsufficientStockException(100L, 3, 1)).when(stockService).decrease(100L, 3);

        consumer.onOrderEvent(MESSAGE);

        verify(eventPublisher).publishStockFailed(order, "재고 부족 productId=100 requested=3 available=1");
        verify(eventPublisher, never()).publishStockReserved(any());
    }

    @Test
    void 재고가_부족해도_예외를_밖으로_던지지_않는다() {
        doThrow(new InsufficientStockException(100L, 3, 1)).when(stockService).decrease(100L, 3);

        assertThatCode(() -> consumer.onOrderEvent(MESSAGE)).doesNotThrowAnyException();
    }
}
