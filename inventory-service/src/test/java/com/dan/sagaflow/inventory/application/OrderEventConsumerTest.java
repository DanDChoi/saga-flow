package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class OrderEventConsumerTest {

    OrderEventConsumer consumer = new OrderEventConsumer(JsonMapper.builder().build());

    @Test
    void ORDER_CREATED_메시지를_역직렬화해_처리한다() {
        String message = """
                {"eventId":"e-1","eventType":"ORDER_CREATED","aggregateId":"7",
                 "occurredAt":"2026-11-03T00:00:00Z",
                 "payload":{"orderId":7,"customerId":1,"productId":100,"quantity":3,"totalAmount":15000}}
                """;

        assertThatCode(() -> consumer.onOrderEvent(message)).doesNotThrowAnyException();
    }
}
