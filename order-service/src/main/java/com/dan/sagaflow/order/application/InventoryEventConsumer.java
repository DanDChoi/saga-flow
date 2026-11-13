package com.dan.sagaflow.order.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.StockFailedPayload;
import com.dan.sagaflow.common.event.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * inventory-events 토픽을 구독해 재고 부족(STOCK_FAILED) 시 주문을 취소한다.
 * 재고 차감이 일어나지 않았으므로 되돌릴 보상은 필요 없고, 주문 상태만 CANCELLED로 바꾸면 된다.
 */
@Component
public class InventoryEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final OrderService orderService;

    public InventoryEventConsumer(JsonMapper jsonMapper, OrderService orderService) {
        this.jsonMapper = jsonMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS)
    public void onInventoryEvent(String message) {
        EventEnvelope<JsonNode> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<JsonNode>>() {});

        if (!StockFailedPayload.EVENT_TYPE.equals(event.eventType())) {
            log.debug("주문 취소와 무관한 이벤트 무시 type={}", event.eventType());
            return;
        }

        StockFailedPayload payload = jsonMapper.treeToValue(event.payload(), StockFailedPayload.class);
        log.warn("재고 부족 수신 -> 주문 취소 orderId={} reason={}", payload.orderId(), payload.reason());
        orderService.cancelOrder(payload.orderId());
    }
}
