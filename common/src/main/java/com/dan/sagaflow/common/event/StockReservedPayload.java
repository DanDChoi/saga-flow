package com.dan.sagaflow.common.event;

/**
 * STOCK_RESERVED 이벤트의 payload. 재고 확보에 성공했음을 알린다.
 * payment-service가 결제할 금액을 알 수 있도록 totalAmount를 그대로 전달한다.
 */
public record StockReservedPayload(
        Long orderId,
        Long productId,
        int quantity,
        long totalAmount
) {

    /** EventEnvelope.eventType 값 */
    public static final String EVENT_TYPE = "STOCK_RESERVED";
}
