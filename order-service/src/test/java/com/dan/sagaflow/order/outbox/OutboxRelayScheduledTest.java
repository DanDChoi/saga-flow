package com.dan.sagaflow.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.application.OrderService;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * 릴레이를 실제 스케줄러로 켠 상태에서 "주문 생성 -> outbox -> 릴레이 -> Kafka" 흐름을 확인한다.
 * Kafka만 목이라 Docker 없이 돈다. 서비스와 릴레이를 이어 주는 설정(스케줄링, 트랜잭션, SKIP LOCKED 쿼리)이
 * 실제로 맞물리는지, 그리고 outbox에 저장된 JSON이 소비자가 읽는 이벤트 형식과 같은지 본다.
 */
@SpringBootTest(properties = {"outbox.relay.enabled=true", "outbox.relay.poll-interval-ms=50"})
@ActiveProfiles("test")
@SuppressWarnings({"unchecked", "rawtypes"})
class OutboxRelayScheduledTest {

    static final Duration TIMEOUT = Duration.ofSeconds(10);

    @MockitoBean
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    JsonMapper jsonMapper;

    @AfterEach
    void clean() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    CompletableFuture ok() {
        return CompletableFuture.completedFuture(mock(SendResult.class, RETURNS_DEEP_STUBS));
    }

    @Test
    void 주문을_만들면_릴레이가_알아서_발행하고_소비자가_읽는_형식의_JSON을_보낸다() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(ok());

        Order order = orderService.createOrder(new CreateOrderRequest(1L, 100L, 2, 5_000L));

        String key = String.valueOf(order.getId());
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        await().atMost(TIMEOUT).untilAsserted(() ->
                verify(kafkaTemplate).send(eq(Topics.ORDER_EVENTS), eq(key), payload.capture()));
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(outboxEventRepository.findAll()).allMatch(OutboxEvent::isPublished));

        // inventory-service가 받는 것과 같은 방식으로 읽어 본다.
        EventEnvelope<OrderCreatedPayload> event =
                jsonMapper.readValue(payload.getValue(), new TypeReference<EventEnvelope<OrderCreatedPayload>>() {});
        assertThat(event.eventType()).isEqualTo("ORDER_CREATED");
        assertThat(event.aggregateId()).isEqualTo(key);
        assertThat(event.payload()).isEqualTo(
                new OrderCreatedPayload(order.getId(), 1L, 100L, 2, 10_000L));
    }

    @Test
    void 발행이_실패해도_주문은_성공하고_브로커가_돌아오면_알아서_다시_발행한다() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 장애")))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 장애")))
                .thenReturn(ok());

        // 브로커가 죽어 있어도 주문 생성은 성공한다. (Day 22에서는 이벤트가 유실됐다)
        Order order = orderService.createOrder(new CreateOrderRequest(1L, 100L, 1, 5_000L));
        assertThat(orderRepository.findById(order.getId())).isPresent();

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(outboxEventRepository.findAll()).allMatch(OutboxEvent::isPublished));
        // 실패 2번 + 성공 1번 = 같은 이벤트를 3번 시도했다.
        verify(kafkaTemplate, atLeast(3)).send(eq(Topics.ORDER_EVENTS), eq(String.valueOf(order.getId())), anyString());
    }
}
