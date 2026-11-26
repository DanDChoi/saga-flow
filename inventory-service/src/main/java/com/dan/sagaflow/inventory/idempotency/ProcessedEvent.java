package com.dan.sagaflow.inventory.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.LocalDateTime;
import org.springframework.data.domain.Persistable;

/**
 * 이미 처리한 이벤트의 기록. eventId가 PK라서 같은 이벤트는 한 번만 기록할 수 있다.
 *
 * <p>컨슈머는 이벤트를 처리하기 전에 이 기록이 있는지 보고(Day 32), 있으면 중복으로 보고 건너뛴다.
 * 기록은 비즈니스 변경(재고 차감)과 같은 트랜잭션에 저장해야 "처리는 됐는데 기록이 없는" 일이 없다.
 *
 * <p>PK를 직접 지정하는 엔티티는 save()가 "새 것인지"를 알 수 없어 기본적으로 먼저 SELECT 후 merge한다.
 * 그러면 같은 eventId를 다시 저장해도 예외 없이 덮어써 버린다. 그래서 Persistable로 항상 새 것임을 알려
 * 중복 저장을 PK 위반 예외로 드러낸다.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent implements Persistable<String> {

    @Id
    private String eventId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false, updatable = false)
    private LocalDateTime processedAt;

    /** DB에서 읽었거나 저장이 끝나면 false. 저장 전(방금 만든 객체)에만 true. */
    @Transient
    private boolean isNew = true;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String eventType) {
        this.eventId = eventId;
        this.eventType = eventType;
    }

    @PrePersist
    void onCreate() {
        this.processedAt = LocalDateTime.now();
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        this.isNew = false;
    }

    @Override
    public String getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
}
