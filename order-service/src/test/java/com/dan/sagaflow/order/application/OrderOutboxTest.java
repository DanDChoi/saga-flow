package com.dan.sagaflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.outbox.OutboxEvent;
import com.dan.sagaflow.order.outbox.OutboxEventRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox 적용 후의 동작. Day 22의 이중 쓰기 문제(OrderDualWriteProblemTest)가 사라졌음을 확인한다.
 * 주문과 이벤트가 같은 DB 트랜잭션에 있으므로 둘은 항상 함께 존재하거나 함께 없다.
 */
@SpringBootTest
@ActiveProfiles("test")
class OrderOutboxTest {

    static final CreateOrderRequest REQUEST = new CreateOrderRequest(1L, 100L, 2, 5_000L);

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @BeforeEach
    void clean() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void 주문을_만들면_같은_트랜잭션에서_ORDER_CREATED가_outbox에_미발행으로_쌓인다() {
        Order order = orderService.createOrder(REQUEST);

        List<OutboxEvent> events = outboxEventRepository.findAll();
        assertThat(events).hasSize(1);
        OutboxEvent event = events.get(0);
        assertThat(event.getTopic()).isEqualTo(Topics.ORDER_EVENTS);
        assertThat(event.getAggregateId()).isEqualTo(String.valueOf(order.getId()));
        assertThat(event.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(event.isPublished()).isFalse();
        // payload는 EventEnvelope 전체 JSON이다.
        assertThat(event.getPayload())
                .contains("\"eventId\":\"" + event.getEventId() + "\"")
                .contains("\"orderId\":" + order.getId())
                .contains("\"totalAmount\":10000");
    }

    @Test
    void 트랜잭션이_롤백되면_주문도_이벤트도_함께_사라진다() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            orderService.createOrder(REQUEST);
            throw new IllegalStateException("커밋 직전 장애");
        })).isInstanceOf(IllegalStateException.class);

        // Day 22에서는 주문은 없는데 이벤트만 나갔다. 이제는 둘 다 없다.
        assertThat(orderRepository.findAll()).isEmpty();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }

    @Test
    void 커밋된_이벤트는_발행이_늦어져도_유실되지_않고_outbox에_남아_있다() {
        orderService.createOrder(REQUEST);

        // Kafka가 죽어 있어도 주문 생성은 성공하고, 발행 대기 중인 이벤트가 남는다. 릴레이가 나중에 보낸다.
        assertThat(outboxEventRepository.findAll()).allMatch(e -> !e.isPublished());
    }
}
