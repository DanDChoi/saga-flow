package com.dan.sagaflow.common.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * 모든 이벤트가 공통으로 사용하는 봉투(envelope).
 *
 * <p>Kafka 메시지의 value에는 항상 이 형식이 담긴다. 서비스마다 payload만 달라진다.
 *
 * @param eventId     이벤트 고유 ID. 멱등성 처리(중복 이벤트 판별)의 기준이 된다.
 * @param eventType   이벤트 종류 (예: ORDER_CREATED). 소비자가 payload를 해석하는 기준이 된다.
 * @param aggregateId 이벤트가 속한 대상의 ID (여기서는 주문 ID). 파티션 키로도 사용한다.
 * @param occurredAt  이벤트가 발생한 시각
 * @param payload     이벤트별 실제 데이터
 */
public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String aggregateId,
        Instant occurredAt,
        T payload
) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }

    /** eventId와 occurredAt을 자동으로 채워 새 이벤트를 만든다. */
    public static <T> EventEnvelope<T> of(String eventType, String aggregateId, T payload) {
        return new EventEnvelope<>(UUID.randomUUID().toString(), eventType, aggregateId, Instant.now(), payload);
    }
}
