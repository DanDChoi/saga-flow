package com.dan.sagaflow.inventory.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.dan.sagaflow.common.event.OrderCreatedPayload;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class OrderEventConsumerTest {

    static final String MESSAGE = """
            {"eventId":"e-1","eventType":"ORDER_CREATED","aggregateId":"7",
             "occurredAt":"2026-11-22T00:00:00Z",
             "payload":{"orderId":7,"customerId":1,"productId":100,"quantity":3,"totalAmount":15000}}
            """;

    StockService stockService = mock(StockService.class);
    OrderEventConsumer consumer = new OrderEventConsumer(JsonMapper.builder().build(), stockService);

    @Test
    void ORDER_CREATED_메시지를_받으면_주문_내용으로_재고를_확보한다() {
        consumer.onOrderEvent(MESSAGE);

        verify(stockService).reserve(new OrderCreatedPayload(7L, 1L, 100L, 3, 15_000L));
    }
}
