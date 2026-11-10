package com.dan.sagaflow.notification.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.PaymentCompletedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
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
 * 사용자에게 결과를 알려야 하는 이벤트(결제 완료/실패, 재고 부족)만 골라 알림으로 기록한다.
 * 이 서비스는 이벤트를 소비만 하고 아무 이벤트도 발행하지 않는다. (Saga의 말단)
 */
@Component
public class NotificationEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final NotificationService notificationService;

    public NotificationEventConsumer(JsonMapper jsonMapper, NotificationService notificationService) {
        this.jsonMapper = jsonMapper;
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = {Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS})
    public void onEvent(String message) {
        EventEnvelope<JsonNode> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<JsonNode>>() {});

        switch (event.eventType()) {
            case PaymentCompletedPayload.EVENT_TYPE -> {
                PaymentCompletedPayload p = jsonMapper.treeToValue(event.payload(), PaymentCompletedPayload.class);
                notificationService.record(p.orderId(), event.eventType(),
                        "주문 %d의 결제가 완료되었습니다. (%d원)".formatted(p.orderId(), p.amount()));
            }
            case PaymentFailedPayload.EVENT_TYPE -> {
                PaymentFailedPayload p = jsonMapper.treeToValue(event.payload(), PaymentFailedPayload.class);
                notificationService.record(p.orderId(), event.eventType(),
                        "주문 %d의 결제에 실패했습니다. 사유: %s".formatted(p.orderId(), p.reason()));
            }
            case StockFailedPayload.EVENT_TYPE -> {
                StockFailedPayload p = jsonMapper.treeToValue(event.payload(), StockFailedPayload.class);
                notificationService.record(p.orderId(), event.eventType(),
                        "주문 %d의 재고를 확보하지 못했습니다. 사유: %s".formatted(p.orderId(), p.reason()));
            }
            default -> log.debug("알림 대상이 아닌 이벤트 무시 type={}", event.eventType());
        }
    }
}
