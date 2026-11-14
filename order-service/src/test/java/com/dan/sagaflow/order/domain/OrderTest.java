package com.dan.sagaflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OrderTest {

    Order newOrder() {
        return Order.create(1L, 100L, 1, 5_000L);
    }

    @Test
    void 새_주문은_완료할_수_있다() {
        Order order = newOrder();

        order.complete();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    @Test
    void 새_주문은_취소할_수_있다() {
        Order order = newOrder();

        order.cancel();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void 취소된_주문은_완료할_수_없고_상태는_그대로다() {
        Order order = newOrder();
        order.cancel();

        assertThatThrownBy(order::complete).isInstanceOf(InvalidOrderStatusTransitionException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void 완료된_주문은_취소할_수_없다() {
        Order order = newOrder();
        order.complete();

        assertThatThrownBy(order::cancel).isInstanceOf(InvalidOrderStatusTransitionException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    @Test
    void 같은_전이를_두_번_할_수_없다() {
        Order order = newOrder();
        order.cancel();

        assertThatThrownBy(order::cancel).isInstanceOf(InvalidOrderStatusTransitionException.class);
    }
}
