package com.dan.sagaflow.order.domain;

/** 허용되지 않는 주문 상태 전이를 시도했을 때. (예: 이미 취소된 주문을 완료 처리) */
public class InvalidOrderStatusTransitionException extends RuntimeException {

    public InvalidOrderStatusTransitionException(Long orderId, OrderStatus from, OrderStatus to) {
        super("허용되지 않는 주문 상태 전이 orderId=%d %s -> %s".formatted(orderId, from, to));
    }
}
