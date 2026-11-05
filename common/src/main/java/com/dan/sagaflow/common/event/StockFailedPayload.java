package com.dan.sagaflow.common.event;

/** STOCK_FAILED 이벤트의 payload. 재고 확보에 실패했음을 알린다. (order-service가 주문을 취소한다) */
public record StockFailedPayload(
        Long orderId,
        Long productId,
        String reason
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "STOCK_FAILED";
}
