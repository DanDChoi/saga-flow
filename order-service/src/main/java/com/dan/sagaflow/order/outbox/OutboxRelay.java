package com.dan.sagaflow.order.outbox;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * outbox 테이블을 주기적으로 읽어 미발행 이벤트를 Kafka로 발행한다. (폴링 방식 릴레이)
 *
 * <p>payload는 이미 직렬화된 JSON이라 해석하지 않고 그대로 보낸다.
 * 발행이 성공한 행만 publishedAt을 채우므로, 실패하면 다음 주기에 다시 시도한다.
 * 발행 직후 DB 반영 전에 죽으면 같은 이벤트가 두 번 나갈 수 있다. (at-least-once)
 *
 * <p>가장 단순한 버전이다. 배치 처리, 트랜잭션 범위, 다중 인스턴스 문제는 Day 26에서 개선한다.
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:500}")
    @Transactional
    public void relay() {
        List<OutboxEvent> pending = outboxEventRepository.findByPublishedAtIsNullOrderByIdAsc();
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                // 순서를 지키기 위해 실패한 행 뒤의 이벤트는 보내지 않고 다음 주기에 이 행부터 다시 시도한다.
                log.warn("outbox 발행 실패, 다음 주기에 재시도 eventId={} type={}", event.getEventId(), event.getEventType(), e);
                return;
            }
            event.markPublished();
            log.info("outbox 발행 eventId={} type={} aggregateId={}", event.getEventId(), event.getEventType(), event.getAggregateId());
        }
    }
}
