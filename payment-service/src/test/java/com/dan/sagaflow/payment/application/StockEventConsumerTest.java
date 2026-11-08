package com.dan.sagaflow.payment.application;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.payment.domain.Payment;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class StockEventConsumerTest {

    static final String RESERVED = """
            {"eventId":"e-1","eventType":"STOCK_RESERVED","aggregateId":"7",
             "occurredAt":"2026-11-07T00:00:00Z",
             "payload":{"orderId":7,"productId":100,"quantity":3,"totalAmount":15000}}
            """;

    PaymentService paymentService = mock(PaymentService.class);
    PaymentEventPublisher eventPublisher = mock(PaymentEventPublisher.class);
    StockEventConsumer consumer =
            new StockEventConsumer(JsonMapper.builder().build(), paymentService, eventPublisher);

    @Test
    void STOCK_RESERVED를_받으면_주문ID와_금액으로_결제한다() {
        when(paymentService.pay(7L, 15_000L)).thenReturn(Payment.completed(7L, 15_000L));

        consumer.onInventoryEvent(RESERVED);

        verify(paymentService).pay(7L, 15_000L);
    }

    @Test
    void 결제에_성공하면_PAYMENT_COMPLETED를_발행한다() {
        Payment payment = Payment.completed(7L, 15_000L);
        when(paymentService.pay(7L, 15_000L)).thenReturn(payment);

        consumer.onInventoryEvent(RESERVED);

        verify(eventPublisher).publishPaymentCompleted(payment);
    }

    @Test
    void 결제가_거절되면_PAYMENT_FAILED를_발행한다() {
        Payment payment = Payment.failed(7L, 15_000L, "결제 한도 초과");
        when(paymentService.pay(7L, 15_000L)).thenReturn(payment);

        consumer.onInventoryEvent(RESERVED);

        verify(eventPublisher).publishPaymentFailed(payment);
    }

    @Test
    void STOCK_FAILED는_payload_형식이_달라도_결제하지_않고_무시한다() {
        String message = """
                {"eventId":"e-2","eventType":"STOCK_FAILED","aggregateId":"7",
                 "occurredAt":"2026-11-07T00:00:00Z",
                 "payload":{"orderId":7,"productId":100,"reason":"재고 부족"}}
                """;

        consumer.onInventoryEvent(message);

        verifyNoInteractions(paymentService, eventPublisher);
    }
}
