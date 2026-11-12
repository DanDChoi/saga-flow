package com.dan.sagaflow.common.event;

/**
 * PAYMENT_FAILED 이벤트의 payload. 결제에 실패했음을 알린다.
 * inventory-service가 재고를 되돌릴 수 있도록 productId와 quantity를 함께 전달한다. (보상 트랜잭션의 시작점)
 */
public record PaymentFailedPayload(
        Long orderId,
        Long productId,
        int quantity,
        String reason
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "PAYMENT_FAILED";
}
