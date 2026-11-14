package com.dan.sagaflow.order.domain;

import java.util.Set;

/**
 * 주문 상태.
 *
 * <p>Saga가 진행되면서 결과 이벤트에 따라 바뀐다.
 * CREATED(처리 중) → COMPLETED(재고/결제 모두 성공) 또는 CANCELLED(어느 단계든 실패).
 * COMPLETED와 CANCELLED는 최종 상태라 더 이상 바뀌지 않는다.
 */
public enum OrderStatus {
    CREATED,
    COMPLETED,
    CANCELLED;

    /** 이 상태에서 바뀔 수 있는 다음 상태들. */
    public Set<OrderStatus> allowedNext() {
        return switch (this) {
            case CREATED -> Set.of(COMPLETED, CANCELLED);
            case COMPLETED, CANCELLED -> Set.of();
        };
    }

    public boolean canTransitionTo(OrderStatus next) {
        return allowedNext().contains(next);
    }
}
