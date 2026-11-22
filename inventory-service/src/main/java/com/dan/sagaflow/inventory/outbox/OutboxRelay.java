package com.dan.sagaflow.inventory.outbox;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * outbox 테이블을 주기적으로 읽어 미발행 이벤트를 Kafka로 발행한다. (폴링 방식 릴레이)
 *
 * <p>payload는 이미 직렬화된 JSON이라 해석하지 않고 그대로 보낸다.
 * 발행이 성공한 행만 publishedAt을 채우므로, 실패하면 다음 주기에 다시 시도한다.
 * 발행 직후 DB 반영 전에 죽으면 같은 이벤트가 두 번 나갈 수 있다. (at-least-once)
 *
 * <p>Day 25 대비 개선점:
 * <ul>
 *   <li>한 번에 batchSize개씩 끊어 처리한다. 배치마다 별도 트랜잭션이라 잠금과 트랜잭션이 길어지지 않는다.</li>
 *   <li>밀린 이벤트가 많으면 다음 주기를 기다리지 않고 이어서 계속 비운다.</li>
 *   <li>SKIP LOCKED로 행을 잠가 가져오므로 인스턴스가 여러 개여도 같은 행을 중복 발행하지 않는다.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;

    public OutboxRelay(OutboxEventRepository outboxEventRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       TransactionTemplate transactionTemplate,
                       @Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:500}")
    public void relay() {
        boolean more;
        do {
            more = Boolean.TRUE.equals(transactionTemplate.execute(status -> publishBatch()));
        } while (more);
    }

    /**
     * 한 배치를 발행한다. 배치가 가득 찼고 전부 성공했을 때만 true(= 바로 이어서 더 처리)를 돌려준다.
     * 실패하면 그 행에서 멈춘다. 순서를 지키기 위해 뒤의 행은 보내지 않고 다음 주기에 그 행부터 다시 시도한다.
     */
    private boolean publishBatch() {
        List<OutboxEvent> batch = outboxEventRepository.findPendingForUpdate(batchSize);
        for (OutboxEvent event : batch) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("outbox 발행 실패, 다음 주기에 재시도 eventId={} type={}", event.getEventId(), event.getEventType(), e);
                return false;
            }
            event.markPublished();
            log.info("outbox 발행 eventId={} type={} aggregateId={}", event.getEventId(), event.getEventType(), event.getAggregateId());
        }
        return batch.size() == batchSize;
    }
}
