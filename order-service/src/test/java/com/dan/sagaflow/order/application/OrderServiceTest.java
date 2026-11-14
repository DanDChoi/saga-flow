package com.dan.sagaflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@ActiveProfiles("test")
class OrderServiceTest {

    // Kafka 브로커 없이 테스트하기 위해 발행기는 목으로 대체한다.
    @MockitoBean
    OrderEventPublisher orderEventPublisher;

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Test
    void 주문을_완료_처리하면_COMPLETED가_된다() {
        Order order = orderRepository.save(Order.create(1L, 100L, 1, 5_000L));

        orderService.completeOrder(order.getId());

        assertThat(orderRepository.findById(order.getId()).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.COMPLETED);
    }

    @Test
    void 주문을_취소_처리하면_CANCELLED가_된다() {
        Order order = orderRepository.save(Order.create(1L, 100L, 1, 5_000L));

        orderService.cancelOrder(order.getId());

        assertThat(orderRepository.findById(order.getId()).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void 취소된_주문에_완료_이벤트가_늦게_와도_예외_없이_취소_상태를_유지한다() {
        Order order = orderRepository.save(Order.create(1L, 100L, 1, 5_000L));
        orderService.cancelOrder(order.getId());

        assertThatCode(() -> orderService.completeOrder(order.getId())).doesNotThrowAnyException();

        assertThat(orderRepository.findById(order.getId()).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void 없는_주문은_예외_없이_넘어간다() {
        assertThatCode(() -> orderService.completeOrder(999_999L)).doesNotThrowAnyException();
    }
}
