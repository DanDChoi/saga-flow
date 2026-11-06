package com.dan.sagaflow.payment.application;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class StockEventConsumerTest {

    StockEventConsumer consumer = new StockEventConsumer(JsonMapper.builder().build());

    @Test
    void STOCK_RESERVED_메시지를_역직렬화해_처리한다() {
        String message = """
                {"eventId":"e-1","eventType":"STOCK_RESERVED","aggregateId":"7",
                 "occurredAt":"2026-11-06T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"quantity":3,"totalAmount":15000}}
                """;

        assertThatCode(() -> consumer.onInventoryEvent(message)).doesNotThrowAnyException();
    }

    @Test
    void STOCK_FAILED_메시지는_payload_형식이_달라도_무시한다() {
        String message = """
                {"eventId":"e-2","eventType":"STOCK_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-06T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"reason":"재고 부족"}}
                """;

        assertThatCode(() -> consumer.onInventoryEvent(message)).doesNotThrowAnyException();
    }
}
