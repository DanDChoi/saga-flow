package com.dan.sagaflow.order.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /** 아직 발행되지 않은 행을 오래된 것(id가 작은 것)부터. */
    List<OutboxEvent> findByPublishedAtIsNullOrderByIdAsc();
}
