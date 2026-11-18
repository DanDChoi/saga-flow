package com.dan.sagaflow.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

@DataJpaTest
@ActiveProfiles("test")
class OutboxEventRepositoryTest {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    OutboxEvent newEvent(String eventId) {
        return new OutboxEvent(eventId, "order-events", "7", "ORDER_CREATED", "{\"eventId\":\"" + eventId + "\"}");
    }

    @Test
    void 저장하면_미발행_상태로_ID와_생성시각이_채워진다() {
        OutboxEvent saved = outboxEventRepository.saveAndFlush(newEvent("e-1"));

        OutboxEvent found = outboxEventRepository.findById(saved.getId()).orElseThrow();

        assertThat(found.getEventId()).isEqualTo("e-1");
        assertThat(found.getTopic()).isEqualTo("order-events");
        assertThat(found.getAggregateId()).isEqualTo("7");
        assertThat(found.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(found.getPayload()).isEqualTo("{\"eventId\":\"e-1\"}");
        assertThat(found.getCreatedAt()).isNotNull();
        assertThat(found.isPublished()).isFalse();
        assertThat(found.getPublishedAt()).isNull();
    }

    @Test
    void 발행_완료로_표시하면_발행시각이_채워진다() {
        OutboxEvent saved = outboxEventRepository.saveAndFlush(newEvent("e-2"));

        saved.markPublished();
        outboxEventRepository.flush();

        assertThat(outboxEventRepository.findById(saved.getId()).orElseThrow().isPublished()).isTrue();
    }

    @Test
    void 같은_eventId는_두_번_저장할_수_없다() {
        outboxEventRepository.saveAndFlush(newEvent("e-3"));

        assertThatThrownBy(() -> outboxEventRepository.saveAndFlush(newEvent("e-3")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void 긴_payload도_저장된다() {
        String longPayload = "x".repeat(5_000);

        OutboxEvent saved = outboxEventRepository.saveAndFlush(
                new OutboxEvent("e-4", "order-events", "7", "ORDER_CREATED", longPayload));

        assertThat(outboxEventRepository.findById(saved.getId()).orElseThrow().getPayload()).hasSize(5_000);
    }
}
