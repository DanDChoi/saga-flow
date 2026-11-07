package com.dan.sagaflow.common.event;

/** PAYMENT_COMPLETED 이벤트의 payload. 결제에 성공했음을 알린다. */
public record PaymentCompletedPayload(
        Long orderId,
        Long paymentId,
        long amount
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "PAYMENT_COMPLETED";
}
