package com.dan.sagaflow.order.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.order.domain.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 주문 이벤트를 Kafka로 직접 발행한다.
 *
 * <p>일부러 가장 단순한 방식이다. DB 저장과 발행이 하나의 트랜잭션이 아니라서
 * 둘 중 하나만 성공할 수 있다(이중 쓰기 문제). Phase 3에서 Outbox로 해결한다.
 */
@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Order order) {
        OrderCreatedPayload payload = new OrderCreatedPayload(
                order.getId(), order.getCustomerId(), order.getProductId(),
                order.getQuantity(), order.getTotalAmount());
        String key = String.valueOf(order.getId()); // 같은 주문의 이벤트는 같은 파티션 -> 순서 보장
        EventEnvelope<OrderCreatedPayload> event =
                EventEnvelope.of(OrderCreatedPayload.EVENT_TYPE, key, payload);

        kafkaTemplate.send(Topics.ORDER_EVENTS, key, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("ORDER_CREATED 발행 실패 orderId={}", order.getId(), ex);
            } else {
                log.info("ORDER_CREATED 발행 orderId={} partition={} offset={}", order.getId(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
