package com.dan.sagaflow.order.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class PaymentEventConsumerTest {

    OrderService orderService = mock(OrderService.class);
    PaymentEventConsumer consumer = new PaymentEventConsumer(JsonMapper.builder().build(), orderService);

    @Test
    void PAYMENT_COMPLETED를_받으면_주문을_완료_처리한다() {
        consumer.onPaymentEvent("""
                {"eventId":"e-1","eventType":"PAYMENT_COMPLETED","aggregateId":"7",
                 "occurredAt":"2026-11-09T00:00:00Z",
                 "payload":{"orderId":7,"paymentId":3,"amount":15000}}
                """);

        verify(orderService).completeOrder(7L);
    }

    @Test
    void PAYMENT_FAILED를_받으면_주문을_취소_처리한다() {
        consumer.onPaymentEvent("""
                {"eventId":"e-2","eventType":"PAYMENT_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-09T00:00:00Z",
                 "payload":{"orderId":7,"reason":"결제 한도 초과"}}
                """);

        verify(orderService).cancelOrder(7L);
    }

    @Test
    void 모르는_이벤트_타입은_무시한다() {
        consumer.onPaymentEvent("""
                {"eventId":"e-3","eventType":"SOMETHING_ELSE","aggregateId":"7",
                 "occurredAt":"2026-11-09T00:00:00Z","payload":{}}
                """);

        verifyNoInteractions(orderService);
    }
}
