package com.dan.sagaflow.payment.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

/**
 * 발행해야 할 이벤트를 DB에 먼저 적어 두는 Outbox 행.
 *
 * <p>비즈니스 데이터(결제)와 같은 DB, 같은 트랜잭션에 저장하므로 "주문은 저장됐는데 이벤트는 사라지는" 일이 없다.
 * 실제 Kafka 발행은 나중에 별도 릴레이가 이 테이블을 읽어서 한다. (Day 25)
 * publishedAt이 null이면 아직 발행되지 않은 행이다.
 */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    /** 릴레이가 오래된 것부터 순서대로 발행할 수 있도록 증가하는 값을 쓴다. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** EventEnvelope.eventId와 같은 값. 소비자 쪽 멱등성의 기준이 된다. */
    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String topic;

    /** Kafka 메시지 키. 같은 주문의 이벤트가 같은 파티션으로 가도록 주문 ID를 쓴다. */
    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    /** EventEnvelope 전체를 직렬화한 JSON. 릴레이는 내용을 해석하지 않고 그대로 발행한다. */
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** 발행 완료 시각. null이면 미발행. */
    private LocalDateTime publishedAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String topic, String aggregateId, String eventType, String payload) {
        this.eventId = eventId;
        this.topic = topic;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
    }

    @PrePersist
    void onCreate() {
        this.createdAt = LocalDateTime.now();
    }

    public void markPublished() {
        this.publishedAt = LocalDateTime.now();
    }

    public boolean isPublished() {
        return publishedAt != null;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getTopic() {
        return topic;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getPublishedAt() {
        return publishedAt;
    }
}
