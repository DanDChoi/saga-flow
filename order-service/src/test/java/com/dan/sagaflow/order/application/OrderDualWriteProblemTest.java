package com.dan.sagaflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.domain.OrderStatus;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 이중 쓰기(dual write) 문제 재현.
 *
 * <p>주문 저장(DB)과 이벤트 발행(Kafka)은 서로 다른 시스템이라 하나의 트랜잭션으로 묶을 수 없다.
 * 그래서 둘 중 하나만 성공하는 상황이 생기고, 두 시스템의 상태가 영영 어긋난다.
 *
 * <p>이 테스트들은 <b>지금의 잘못된 동작을 그대로 단언</b>한다. 즉 "통과"는 문제가 재현된다는 뜻이다.
 * Outbox 패턴을 적용하면(Day 24~25) 이 동작은 사라지므로, 그때 이 테스트를 뒤집거나 정상 동작 테스트로 바꾼다.
 */
@SpringBootTest
@ActiveProfiles("test")
@SuppressWarnings({"unchecked", "rawtypes"})
class OrderDualWriteProblemTest {

    static final CreateOrderRequest REQUEST = new CreateOrderRequest(1L, 100L, 2, 5_000L);

    @MockitoBean
    KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @BeforeEach
    void cleanOrders() {
        orderRepository.deleteAll();
    }

    @Test
    void 발행은_성공했는데_DB_트랜잭션이_롤백되면_존재하지_않는_주문의_이벤트가_나간다() {
        when(kafkaTemplate.send(anyString(), anyString(), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.completedFuture(mockSendResult()));

        // createOrder는 이미 이벤트를 발행한 뒤다. 그 직후 같은 트랜잭션에서 장애가 나 롤백되는 상황을 흉내 낸다.
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            orderService.createOrder(REQUEST);
            throw new IllegalStateException("발행 이후 커밋 직전 장애");
        })).isInstanceOf(IllegalStateException.class);

        // DB에는 주문이 없다...
        assertThat(orderRepository.findAll()).isEmpty();
        // ...하지만 ORDER_CREATED 이벤트는 이미 Kafka로 나갔다. 소비자들은 없는 주문을 처리하게 된다.
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any(EventEnvelope.class));
    }

    @Test
    void DB는_커밋됐는데_발행이_실패하면_이벤트가_유실되고_주문은_CREATED에_영원히_머문다() {
        when(kafkaTemplate.send(anyString(), anyString(), any(EventEnvelope.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 연결 실패")));

        // 발행 실패는 비동기 콜백에서 로그만 남기므로 주문 생성은 아무 일 없다는 듯 성공한다.
        Order order = orderService.createOrder(REQUEST);

        // DB에는 주문이 커밋돼 있다...
        Order saved = orderRepository.findById(order.getId()).orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(OrderStatus.CREATED);
        // ...하지만 이벤트는 나가지 못했고 재시도하는 곳도 없다. 재고/결제 단계가 시작되지 않아 주문은 영원히 멈춘다.
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any(EventEnvelope.class));
    }

    private SendResult mockSendResult() {
        return mock(SendResult.class, RETURNS_DEEP_STUBS);
    }
}
