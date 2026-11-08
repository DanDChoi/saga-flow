package com.dan.sagaflow.payment.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.PaymentCompletedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.payment.domain.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 결제 결과를 payment-events 토픽으로 직접 발행한다.
 * 다른 서비스와 마찬가지로 Outbox 적용 전의 단순한 방식이다. (Day 28에서 개선)
 */
@Component
public class PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventPublisher.class);

    private final KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;

    public PaymentEventPublisher(KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentCompleted(Payment payment) {
        publish(EventEnvelope.of(PaymentCompletedPayload.EVENT_TYPE, String.valueOf(payment.getOrderId()),
                new PaymentCompletedPayload(payment.getOrderId(), payment.getId(), payment.getAmount())));
    }

    public void publishPaymentFailed(Payment payment) {
        publish(EventEnvelope.of(PaymentFailedPayload.EVENT_TYPE, String.valueOf(payment.getOrderId()),
                new PaymentFailedPayload(payment.getOrderId(), payment.getFailureReason())));
    }

    private void publish(EventEnvelope<?> event) {
        // 키는 aggregateId(주문 ID). 같은 주문의 이벤트는 같은 파티션으로 간다.
        kafkaTemplate.send(Topics.PAYMENT_EVENTS, event.aggregateId(), event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("{} 발행 실패 orderId={}", event.eventType(), event.aggregateId(), ex);
            } else {
                log.info("{} 발행 orderId={} partition={} offset={}", event.eventType(), event.aggregateId(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
