package com.dan.sagaflow.order.outbox;

import com.dan.sagaflow.common.event.EventEnvelope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * 이벤트를 Kafka로 보내지 않고 outbox 테이블에 기록한다.
 *
 * <p>반드시 호출자의 트랜잭션 안에서 실행되어야 한다(MANDATORY). 그래야 비즈니스 데이터와 이벤트가
 * 한 트랜잭션으로 함께 커밋/롤백된다. 트랜잭션 없이 호출하면 예외가 나서 실수를 바로 알 수 있다.
 */
@Component
public class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    public OutboxWriter(OutboxEventRepository outboxEventRepository, JsonMapper jsonMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, EventEnvelope<?> event) {
        outboxEventRepository.save(new OutboxEvent(
                event.eventId(), topic, event.aggregateId(), event.eventType(), jsonMapper.writeValueAsString(event)));
    }
}
