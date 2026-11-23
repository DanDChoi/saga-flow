package com.dan.sagaflow.payment.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.dan.sagaflow.common.event.StockReservedPayload;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class StockEventConsumerTest {

    PaymentService paymentService = mock(PaymentService.class);
    StockEventConsumer consumer = new StockEventConsumer(JsonMapper.builder().build(), paymentService);

    @Test
    void STOCK_RESERVED를_받으면_결제한다() {
        consumer.onInventoryEvent("""
                {"eventId":"e-1","eventType":"STOCK_RESERVED","aggregateId":"7",
                 "occurredAt":"2026-11-23T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"quantity":3,"totalAmount":15000}}
                """);

        verify(paymentService).pay(new StockReservedPayload(7L, 100L, 3, 15_000L));
    }

    @Test
    void STOCK_FAILED는_payload_형식이_달라도_결제하지_않고_무시한다() {
        consumer.onInventoryEvent("""
                {"eventId":"e-2","eventType":"STOCK_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-23T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"reason":"재고 부족"}}
                """);

        verifyNoInteractions(paymentService);
    }
}
