package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * order-events 토픽을 구독한다. 아직은 받은 이벤트를 로그로만 남긴다. (재고 차감은 Day 10)
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final JsonMapper jsonMapper;

    public OrderEventConsumer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS)
    public void onOrderEvent(String message) {
        EventEnvelope<OrderCreatedPayload> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<OrderCreatedPayload>>() {});
        log.info("주문 이벤트 수신 eventId={} type={} payload={}", event.eventId(), event.eventType(), event.payload());
    }
}
