package com.dan.sagaflow.notification.application;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class NotificationEventConsumerTest {

    NotificationService notificationService = mock(NotificationService.class);
    NotificationEventConsumer consumer =
            new NotificationEventConsumer(JsonMapper.builder().build(), notificationService);

    @Test
    void PAYMENT_COMPLETED는_결제_완료_알림으로_기록한다() {
        consumer.onEvent(event("PAYMENT_COMPLETED", """
                {"orderId":7,"paymentId":3,"amount":15000}"""));

        verify(notificationService).record(7L, "PAYMENT_COMPLETED", "주문 7의 결제가 완료되었습니다. (15000원)");
    }

    @Test
    void PAYMENT_FAILED는_사유를_담아_기록한다() {
        consumer.onEvent(event("PAYMENT_FAILED", """
                {"orderId":7,"reason":"결제 한도 초과"}"""));

        verify(notificationService).record(7L, "PAYMENT_FAILED", "주문 7의 결제에 실패했습니다. 사유: 결제 한도 초과");
    }

    @Test
    void STOCK_FAILED는_재고_부족_알림으로_기록한다() {
        consumer.onEvent(event("STOCK_FAILED", """
                {"orderId":7,"productId":100,"reason":"재고 부족"}"""));

        verify(notificationService).record(eq(7L), eq("STOCK_FAILED"), contains("재고를 확보하지 못했습니다"));
    }

    @Test
    void 알림_대상이_아닌_이벤트는_무시한다() {
        consumer.onEvent(event("STOCK_RESERVED", """
                {"orderId":7,"productId":100,"quantity":3,"totalAmount":15000}"""));

        verifyNoInteractions(notificationService);
    }

    private String event(String type, String payload) {
        return """
                {"eventId":"e-1","eventType":"%s","aggregateId":"7",
                 "occurredAt":"2026-11-10T00:00:00Z","payload":%s}
                """.formatted(type, payload);
    }
}
