package com.dan.sagaflow.order.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.PaymentCompletedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * payment-events 토픽을 구독해 결제 결과로 주문 상태를 갱신한다.
 * (재고 부족으로 인한 취소는 {@link InventoryEventConsumer}가 처리한다)
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final OrderService orderService;

    public PaymentEventConsumer(JsonMapper jsonMapper, OrderService orderService) {
        this.jsonMapper = jsonMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onPaymentEvent(String message) {
        EventEnvelope<JsonNode> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<JsonNode>>() {});

        switch (event.eventType()) {
            case PaymentCompletedPayload.EVENT_TYPE -> {
                PaymentCompletedPayload payload = jsonMapper.treeToValue(event.payload(), PaymentCompletedPayload.class);
                log.info("결제 완료 수신 -> 주문 완료 orderId={}", payload.orderId());
                orderService.completeOrder(payload.orderId());
            }
            case PaymentFailedPayload.EVENT_TYPE -> {
                PaymentFailedPayload payload = jsonMapper.treeToValue(event.payload(), PaymentFailedPayload.class);
                log.warn("결제 실패 수신 -> 주문 취소 orderId={} reason={}", payload.orderId(), payload.reason());
                orderService.cancelOrder(payload.orderId());
            }
            default -> log.debug("주문과 무관한 이벤트 무시 type={}", event.eventType());
        }
    }
}
