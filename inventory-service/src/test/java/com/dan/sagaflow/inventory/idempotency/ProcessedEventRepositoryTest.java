package com.dan.sagaflow.inventory.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

@DataJpaTest
@ActiveProfiles("test")
class ProcessedEventRepositoryTest {

    @Autowired
    ProcessedEventRepository processedEventRepository;

    @Test
    void 처리한_이벤트를_기록하면_eventId로_조회된다() {
        processedEventRepository.saveAndFlush(new ProcessedEvent("e-1", "ORDER_CREATED"));

        assertThat(processedEventRepository.existsById("e-1")).isTrue();
        assertThat(processedEventRepository.existsById("e-other")).isFalse();

        ProcessedEvent found = processedEventRepository.findById("e-1").orElseThrow();
        assertThat(found.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(found.getProcessedAt()).isNotNull();
    }

    @Test
    void 같은_eventId를_두_번_기록하면_PK_위반으로_실패한다() {
        processedEventRepository.saveAndFlush(new ProcessedEvent("e-2", "ORDER_CREATED"));

        // 덮어쓰지 않고 예외가 나야 중복 처리를 감지할 수 있다.
        assertThatThrownBy(() -> processedEventRepository.saveAndFlush(new ProcessedEvent("e-2", "ORDER_CREATED")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void 서로_다른_eventId는_각각_기록할_수_있다() {
        processedEventRepository.saveAndFlush(new ProcessedEvent("e-3", "ORDER_CREATED"));
        processedEventRepository.saveAndFlush(new ProcessedEvent("e-4", "PAYMENT_FAILED"));

        assertThat(processedEventRepository.count()).isEqualTo(2);
    }
}
