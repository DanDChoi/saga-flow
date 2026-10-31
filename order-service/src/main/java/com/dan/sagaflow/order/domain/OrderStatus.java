package com.dan.sagaflow.order.domain;

/**
 * 주문 상태.
 *
 * <p>Saga가 진행되면서 결과 이벤트에 따라 바뀐다.
 * CREATED(처리 중) → COMPLETED(재고/결제 모두 성공) 또는 CANCELLED(어느 단계든 실패).
 */
public enum OrderStatus {
    CREATED,
    COMPLETED,
    CANCELLED
}
