package com.dan.sagaflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OrderStatusTest {

    @Test
    void CREATED는_COMPLETED나_CANCELLED로만_바뀐다() {
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.COMPLETED)).isTrue();
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.CANCELLED)).isTrue();
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.CREATED)).isFalse();
    }

    @Test
    void 최종_상태는_어디로도_바뀌지_않는다() {
        for (OrderStatus from : new OrderStatus[] {OrderStatus.COMPLETED, OrderStatus.CANCELLED}) {
            for (OrderStatus to : OrderStatus.values()) {
                assertThat(from.canTransitionTo(to)).as("%s -> %s", from, to).isFalse();
            }
        }
    }
}
