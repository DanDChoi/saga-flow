package com.dan.sagaflow.common.event;

/** PAYMENT_FAILED 이벤트의 payload. 결제에 실패했음을 알린다. (재고 복구 보상의 시작점이 된다) */
public record PaymentFailedPayload(
        Long orderId,
        String reason
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "PAYMENT_FAILED";
}
