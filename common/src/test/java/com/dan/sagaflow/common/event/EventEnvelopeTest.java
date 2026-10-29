package com.dan.sagaflow.common.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EventEnvelopeTest {

    @Test
    void of는_eventId와_발생시각을_자동으로_채운다() {
        EventEnvelope<String> event = EventEnvelope.of("ORDER_CREATED", "order-1", "payload");

        assertThat(event.eventId()).isNotBlank();
        assertThat(event.occurredAt()).isNotNull();
        assertThat(event.eventType()).isEqualTo("ORDER_CREATED");
        assertThat(event.aggregateId()).isEqualTo("order-1");
        assertThat(event.payload()).isEqualTo("payload");
    }

    @Test
    void 같은_내용으로_만들어도_eventId는_매번_다르다() {
        EventEnvelope<String> first = EventEnvelope.of("ORDER_CREATED", "order-1", "payload");
        EventEnvelope<String> second = EventEnvelope.of("ORDER_CREATED", "order-1", "payload");

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }

    @Test
    void aggregateId가_없으면_생성할_수_없다() {
        assertThatThrownBy(() -> EventEnvelope.of("ORDER_CREATED", null, "payload"))
                .isInstanceOf(NullPointerException.class);
    }
}
