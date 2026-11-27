package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.common.event.EventEnvelope;
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
 * payment-events 토픽을 구독한다. 결제가 실패하면 앞서 차감한 재고를 되돌린다(보상).
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final StockService stockService;

    public PaymentEventConsumer(JsonMapper jsonMapper, StockService stockService) {
        this.jsonMapper = jsonMapper;
        this.stockService = stockService;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onPaymentEvent(String message) {
        EventEnvelope<JsonNode> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<JsonNode>>() {});

        if (!PaymentFailedPayload.EVENT_TYPE.equals(event.eventType())) {
            log.debug("재고와 무관한 이벤트 무시 type={}", event.eventType());
            return;
        }

        PaymentFailedPayload payload = jsonMapper.treeToValue(event.payload(), PaymentFailedPayload.class);
        stockService.release(event.eventId(), payload);
    }
}
