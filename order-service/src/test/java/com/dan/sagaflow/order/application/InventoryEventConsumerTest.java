package com.dan.sagaflow.order.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class InventoryEventConsumerTest {

    OrderService orderService = mock(OrderService.class);
    InventoryEventConsumer consumer = new InventoryEventConsumer(JsonMapper.builder().build(), orderService);

    @Test
    void STOCK_FAILED를_받으면_주문을_취소_처리한다() {
        consumer.onInventoryEvent("""
                {"eventId":"e-1","eventType":"STOCK_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-13T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"reason":"재고 부족"}}
                """);

        verify(orderService).cancelOrder(7L);
    }

    @Test
    void 재고_확보_이벤트는_무시한다() {
        consumer.onInventoryEvent("""
                {"eventId":"e-2","eventType":"STOCK_RESERVED","aggregateId":"7",
                 "occurredAt":"2026-11-13T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"quantity":3,"totalAmount":15000}}
                """);

        verifyNoInteractions(orderService);
    }

    @Test
    void 재고_복구_이벤트는_무시한다() {
        consumer.onInventoryEvent("""
                {"eventId":"e-3","eventType":"STOCK_RELEASED","aggregateId":"7",
                 "occurredAt":"2026-11-13T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"quantity":3}}
                """);

        verifyNoInteractions(orderService);
    }
}
