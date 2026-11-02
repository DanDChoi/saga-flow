package com.dan.sagaflow.common.event;

/**
 * ORDER_CREATED 이벤트의 payload. inventory-service가 재고를 차감하는 데 필요한 정보를 담는다.
 */
public record OrderCreatedPayload(
        Long orderId,
        Long customerId,
        Long productId,
        int quantity,
        long totalAmount
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "ORDER_CREATED";
}
