package com.dan.sagaflow.inventory.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.dan.sagaflow.common.event.PaymentFailedPayload;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class PaymentEventConsumerTest {

    StockService stockService = mock(StockService.class);
    InventoryEventPublisher eventPublisher = mock(InventoryEventPublisher.class);
    PaymentEventConsumer consumer =
            new PaymentEventConsumer(JsonMapper.builder().build(), stockService, eventPublisher);

    @Test
    void PAYMENT_FAILED를_받으면_재고를_되돌리고_STOCK_RELEASED를_발행한다() {
        consumer.onPaymentEvent("""
                {"eventId":"e-1","eventType":"PAYMENT_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-12T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"quantity":3,"reason":"결제 한도 초과"}}
                """);

        verify(stockService).increase(100L, 3);
        verify(eventPublisher).publishStockReleased(new PaymentFailedPayload(7L, 100L, 3, "결제 한도 초과"));
    }

    @Test
    void 결제_성공_이벤트는_무시한다() {
        consumer.onPaymentEvent("""
                {"eventId":"e-2","eventType":"PAYMENT_COMPLETED","aggregateId":"7",
                 "occurredAt":"2026-11-12T00:00:00Z",
                 "payload":{"orderId":7,"paymentId":3,"amount":15000}}
                """);

        verifyNoInteractions(stockService, eventPublisher);
    }
}
