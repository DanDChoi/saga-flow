package com.dan.sagaflow.common.event;

/** STOCK_RELEASED 이벤트의 payload. 결제 실패로 차감했던 재고를 되돌렸음을 알린다. (보상 완료) */
public record StockReleasedPayload(
        Long orderId,
        Long productId,
        int quantity
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "STOCK_RELEASED";
}
