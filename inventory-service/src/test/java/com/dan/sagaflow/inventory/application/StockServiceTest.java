package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import com.dan.sagaflow.inventory.outbox.OutboxEvent;
import com.dan.sagaflow.inventory.outbox.OutboxEventRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/** 테스트끼리 간섭하지 않도록 테스트마다 다른 상품 ID와 주문 ID를 쓴다. (outbox는 주문 ID로 걸러 본다) */
@SpringBootTest
@ActiveProfiles("test")
class StockServiceTest {

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    List<OutboxEvent> outboxOf(long orderId) {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(String.valueOf(orderId))).toList();
    }

    @Test
    void 재고를_확보하면_재고가_줄고_STOCK_RESERVED가_미발행으로_outbox에_쌓인다() {
        stockRepository.save(new Stock(1L, 10));

        stockService.reserve(new OrderCreatedPayload(101L, 1L, 1L, 4, 20_000L));

        assertThat(stockRepository.findById(1L).orElseThrow().getQuantity()).isEqualTo(6);
        List<OutboxEvent> events = outboxOf(101L);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getEventType()).isEqualTo("STOCK_RESERVED");
        assertThat(events.get(0).getTopic()).isEqualTo(Topics.INVENTORY_EVENTS);
        assertThat(events.get(0).isPublished()).isFalse();
        assertThat(events.get(0).getPayload()).contains("\"totalAmount\":20000");
    }

    @Test
    void 재고가_부족하면_재고는_그대로고_STOCK_FAILED가_outbox에_쌓인다() {
        stockRepository.save(new Stock(2L, 1));

        stockService.reserve(new OrderCreatedPayload(102L, 1L, 2L, 2, 10_000L));

        assertThat(stockRepository.findById(2L).orElseThrow().getQuantity()).isEqualTo(1);
        List<OutboxEvent> events = outboxOf(102L);
        assertThat(events).extracting(OutboxEvent::getEventType).containsExactly("STOCK_FAILED");
        assertThat(events.get(0).getPayload()).contains("재고 부족");
    }

    @Test
    void 재고_행이_없는_상품도_STOCK_FAILED로_처리한다() {
        stockService.reserve(new OrderCreatedPayload(103L, 1L, 999L, 1, 1_000L));

        assertThat(outboxOf(103L)).extracting(OutboxEvent::getEventType).containsExactly("STOCK_FAILED");
    }

    @Test
    void 트랜잭션이_롤백되면_재고_변경도_이벤트도_함께_사라진다() {
        stockRepository.save(new Stock(3L, 10));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            stockService.reserve(new OrderCreatedPayload(104L, 1L, 3L, 4, 20_000L));
            throw new IllegalStateException("커밋 직전 장애");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(stockRepository.findById(3L).orElseThrow().getQuantity()).isEqualTo(10);
        assertThat(outboxOf(104L)).isEmpty();
    }

    @Test
    void 차감했던_재고를_되돌리면_재고가_복구되고_STOCK_RELEASED가_outbox에_쌓인다() {
        stockRepository.save(new Stock(4L, 10));
        stockService.reserve(new OrderCreatedPayload(105L, 1L, 4L, 4, 20_000L));

        stockService.release(new PaymentFailedPayload(105L, 4L, 4, "결제 한도 초과"));

        assertThat(stockRepository.findById(4L).orElseThrow().getQuantity()).isEqualTo(10);
        assertThat(outboxOf(105L)).extracting(OutboxEvent::getEventType)
                .containsExactlyInAnyOrder("STOCK_RESERVED", "STOCK_RELEASED");
    }

    @Test
    void 재고_행이_없는_상품은_복구할_수_없고_이벤트도_남기지_않는다() {
        assertThatThrownBy(() -> stockService.release(new PaymentFailedPayload(106L, 998L, 1, "결제 실패")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(outboxOf(106L)).isEmpty();
    }
}
