package com.dan.sagaflow.payment.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * 미발행 행을 오래된 것부터 최대 limit개 가져오면서 행 잠금을 건다.
     *
     * <p>SKIP LOCKED: 다른 릴레이 인스턴스가 이미 잠근 행은 기다리지 않고 건너뛴다.
     * 그래서 릴레이가 여러 개 떠 있어도 같은 행을 중복 발행하지 않고 서로 다른 행을 나눠 처리한다.
     * 잠금은 트랜잭션이 끝날 때 풀리므로 반드시 트랜잭션 안에서 호출해야 한다.
     */
    @Query(value = """
            SELECT * FROM outbox_event
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingForUpdate(@Param("limit") int limit);
}
