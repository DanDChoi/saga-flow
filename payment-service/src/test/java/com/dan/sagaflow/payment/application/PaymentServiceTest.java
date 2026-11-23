package com.dan.sagaflow.payment.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentRepository;
import com.dan.sagaflow.payment.domain.PaymentStatus;
import com.dan.sagaflow.payment.outbox.OutboxEvent;
import com.dan.sagaflow.payment.outbox.OutboxEventRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/** 테스트끼리 간섭하지 않도록 테스트마다 다른 주문 ID를 쓴다. (outbox는 주문 ID로 걸러 본다) */
@SpringBootTest
@ActiveProfiles("test")
class PaymentServiceTest {

    @Autowired
    PaymentService paymentService;

    @Autowired
    PaymentRepository paymentRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    List<OutboxEvent> outboxOf(long orderId) {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(String.valueOf(orderId))).toList();
    }

    @Test
    void 결제에_성공하면_COMPLETED로_저장되고_PAYMENT_COMPLETED가_미발행으로_outbox에_쌓인다() {
        Payment payment = paymentService.pay(new StockReservedPayload(1L, 100L, 3, 15_000L));

        Payment found = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(found.getOrderId()).isEqualTo(1L);
        assertThat(found.getAmount()).isEqualTo(15_000L);
        assertThat(found.getFailureReason()).isNull();
        assertThat(found.getCreatedAt()).isNotNull();

        List<OutboxEvent> events = outboxOf(1L);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getEventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(events.get(0).getTopic()).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(events.get(0).isPublished()).isFalse();
        assertThat(events.get(0).getPayload()).contains("\"paymentId\":" + payment.getId());
    }

    @Test
    void 결제가_거절되면_FAILED로_저장되고_재고_복구용_정보를_담은_PAYMENT_FAILED가_쌓인다() {
        Payment payment = paymentService.pay(new StockReservedPayload(2L, 100L, 3, 5_000_000L));

        Payment found = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(found.getFailureReason()).contains("한도 초과");

        List<OutboxEvent> events = outboxOf(2L);
        assertThat(events).extracting(OutboxEvent::getEventType).containsExactly("PAYMENT_FAILED");
        assertThat(events.get(0).getPayload())
                .contains("\"productId\":100")
                .contains("\"quantity\":3")
                .contains("한도 초과");
    }

    @Test
    void 트랜잭션이_롤백되면_결제_기록도_이벤트도_함께_사라진다() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            paymentService.pay(new StockReservedPayload(3L, 100L, 1, 5_000L));
            throw new IllegalStateException("커밋 직전 장애");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(paymentRepository.findAll().stream().filter(p -> p.getOrderId().equals(3L))).isEmpty();
        assertThat(outboxOf(3L)).isEmpty();
    }
}
