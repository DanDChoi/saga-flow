package com.dan.sagaflow.payment.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * inventory-events 토픽을 구독한다. STOCK_RESERVED만 골라 결제하고, 결과(성공/실패)를 이벤트로 발행한다.
 */
@Component
public class StockEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockEventConsumer.class);

    private final JsonMapper jsonMapper;

    private final PaymentService paymentService;
    private final PaymentEventPublisher eventPublisher;

    public StockEventConsumer(JsonMapper jsonMapper, PaymentService paymentService,
                              PaymentEventPublisher eventPublisher) {
        this.jsonMapper = jsonMapper;
        this.paymentService = paymentService;
        this.eventPublisher = eventPublisher;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS)
    public void onInventoryEvent(String message) {
        // 같은 토픽에 STOCK_FAILED도 흐르므로, payload는 JsonNode로 두고 eventType을 먼저 확인한다.
        EventEnvelope<JsonNode> event =
                jsonMapper.readValue(message, new TypeReference<EventEnvelope<JsonNode>>() {});

        if (!StockReservedPayload.EVENT_TYPE.equals(event.eventType())) {
            log.debug("결제와 무관한 이벤트 무시 type={}", event.eventType());
            return;
        }

        StockReservedPayload payload = jsonMapper.treeToValue(event.payload(), StockReservedPayload.class);
        log.info("재고 확보 이벤트 수신 eventId={} payload={}", event.eventId(), payload);

        Payment payment = paymentService.pay(payload.orderId(), payload.totalAmount());
        if (payment.getStatus() == PaymentStatus.COMPLETED) {
            eventPublisher.publishPaymentCompleted(payment);
        } else {
            log.warn("결제 실패 orderId={} reason={}", payment.getOrderId(), payment.getFailureReason());
            eventPublisher.publishPaymentFailed(payment);
        }
    }
}
