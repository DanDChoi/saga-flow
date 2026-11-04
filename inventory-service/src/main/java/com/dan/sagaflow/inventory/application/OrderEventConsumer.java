package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.inventory.domain.InsufficientStockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * order-events 토픽을 구독한다. 받은 주문의 재고를 차감한다. 결과 이벤트 발행은 Day 11.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final StockService stockService;

    public OrderEventConsumer(JsonMapper jsonMapper, StockService stockService) {
        this.jsonMapper = jsonMapper;
        this.stockService = stockService;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS)
    public void onOrderEvent(String message) {
        EventEnvelope<OrderCreatedPayload> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<OrderCreatedPayload>>() {});
        log.info("주문 이벤트 수신 eventId={} type={} payload={}", event.eventId(), event.eventType(), event.payload());

        OrderCreatedPayload order = event.payload();
        try {
            stockService.decrease(order.productId(), order.quantity());
            log.info("재고 차감 완료 orderId={}", order.orderId());
        } catch (InsufficientStockException e) {
            log.warn("재고 차감 실패 orderId={} reason={}", order.orderId(), e.getMessage());
        }
    }
}
