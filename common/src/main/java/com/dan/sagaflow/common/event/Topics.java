package com.dan.sagaflow.common.event;

/**
 * Kafka 토픽 이름.
 *
 * <p>토픽은 "이벤트를 발행하는 서비스" 기준으로 나눈다.
 * 각 서비스는 자기 토픽에만 발행하고, 필요한 다른 서비스의 토픽을 구독한다.
 */
public final class Topics {

    public static final String ORDER_EVENTS = "order-events";
    public static final String INVENTORY_EVENTS = "inventory-events";
    public static final String PAYMENT_EVENTS = "payment-events";

    private Topics() {
    }
}
