package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import com.dan.sagaflow.inventory.outbox.OutboxEvent;
import com.dan.sagaflow.inventory.outbox.OutboxEventRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * 중복 이벤트 문제 재현.
 *
 * <p>Kafka(와 Outbox 릴레이)는 at-least-once라서 같은 메시지가 두 번 이상 도착할 수 있다.
 * 예: 컨슈머가 처리는 끝냈지만 offset 커밋 전에 죽거나 리밸런싱이 일어나면, 같은 메시지를 다시 받는다.
 * 지금의 컨슈머는 "이 이벤트를 이미 처리했는가"를 확인하지 않아서 중복을 그대로 다시 처리한다.
 *
 * <p>이 테스트는 <b>지금의 잘못된 동작을 그대로 단언</b>한다. 즉 "통과"는 문제가 재현된다는 뜻이다.
 * 멱등 컨슈머를 적용하면(Day 31~32) 이 동작은 사라지므로, 그때 이 테스트를 정상 동작 테스트로 바꾼다.
 */
@SpringBootTest
@ActiveProfiles("test")
class DuplicateEventProblemTest {

    /** 같은 eventId("e-dup")를 가진, 주문 7번에 대한 ORDER_CREATED. 상품 400번을 3개 주문한다. */
    static final String MESSAGE = """
            {"eventId":"e-dup","eventType":"ORDER_CREATED","aggregateId":"7",
             "occurredAt":"2026-11-25T00:00:00Z",
             "payload":{"orderId":7,"customerId":1,"productId":400,"quantity":3,"totalAmount":15000}}
            """;

    @Autowired
    OrderEventConsumer consumer;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Test
    void 같은_주문_이벤트가_두_번_도착하면_재고가_두_번_차감된다() {
        stockRepository.save(new Stock(400L, 10));

        consumer.onOrderEvent(MESSAGE);
        consumer.onOrderEvent(MESSAGE); // 같은 메시지 재전달

        // 주문은 한 번인데 재고는 3개가 아니라 6개가 빠졌다. (정상이라면 10 - 3 = 7이 남아야 한다)
        assertThat(stockRepository.findById(400L).orElseThrow().getQuantity()).isEqualTo(10 - 3 * 2);
    }

    @Test
    void 중복_처리는_STOCK_RESERVED도_두_번_만들어_결제까지_중복시킨다() {
        stockRepository.save(new Stock(400L, 10));
        outboxEventRepository.deleteAll();

        consumer.onOrderEvent(MESSAGE);
        consumer.onOrderEvent(MESSAGE);

        // 주문 7번에 대한 STOCK_RESERVED가 서로 다른 eventId로 두 개 발행 대기 중이다.
        // 소비자(payment-service)는 두 이벤트를 별개로 보므로 같은 주문을 두 번 결제하게 된다.
        List<OutboxEvent> reserved = outboxEventRepository.findAll().stream()
                .filter(e -> e.getEventType().equals("STOCK_RESERVED") && e.getAggregateId().equals("7"))
                .toList();
        assertThat(reserved).hasSize(2);
        assertThat(reserved).extracting(OutboxEvent::getEventId).doesNotHaveDuplicates();
    }
}
