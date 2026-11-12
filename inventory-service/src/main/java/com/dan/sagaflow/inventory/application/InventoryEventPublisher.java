package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.StockFailedPayload;
import com.dan.sagaflow.common.event.StockReleasedPayload;
import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 재고 처리 결과를 inventory-events 토픽으로 직접 발행한다.
 * order-service와 마찬가지로 Outbox 적용 전의 단순한 방식이다. (Day 27에서 개선)
 */
@Component
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);

    private final KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;

    public InventoryEventPublisher(KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockReserved(OrderCreatedPayload order) {
        publish(EventEnvelope.of(StockReservedPayload.EVENT_TYPE, String.valueOf(order.orderId()),
                new StockReservedPayload(order.orderId(), order.productId(), order.quantity(), order.totalAmount())));
    }

    public void publishStockFailed(OrderCreatedPayload order, String reason) {
        publish(EventEnvelope.of(StockFailedPayload.EVENT_TYPE, String.valueOf(order.orderId()),
                new StockFailedPayload(order.orderId(), order.productId(), reason)));
    }

    public void publishStockReleased(PaymentFailedPayload failed) {
        publish(EventEnvelope.of(StockReleasedPayload.EVENT_TYPE, String.valueOf(failed.orderId()),
                new StockReleasedPayload(failed.orderId(), failed.productId(), failed.quantity())));
    }

    private void publish(EventEnvelope<?> event) {
        // 키는 aggregateId(주문 ID). 같은 주문의 이벤트는 같은 파티션으로 간다.
        kafkaTemplate.send(Topics.INVENTORY_EVENTS, event.aggregateId(), event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("{} 발행 실패 orderId={}", event.eventType(), event.aggregateId(), ex);
            } else {
                log.info("{} 발행 orderId={} partition={} offset={}", event.eventType(), event.aggregateId(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
