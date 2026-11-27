package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import com.dan.sagaflow.inventory.idempotency.ProcessedEventRepository;
import com.dan.sagaflow.inventory.outbox.OutboxEvent;
import com.dan.sagaflow.inventory.outbox.OutboxEventRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 멱등 컨슈머. Day 30의 DuplicateEventProblemTest가 재현하던 "같은 이벤트가 두 번 오면 두 번 처리됨"이
 * 사라졌음을 확인한다. 테스트마다 다른 상품/주문/이벤트 ID를 쓴다.
 */
@SpringBootTest
@ActiveProfiles("test")
class IdempotentConsumerTest {

    static String orderMessage(String eventId, long orderId, long productId) {
        return """
                {"eventId":"%s","eventType":"ORDER_CREATED","aggregateId":"%d",
                 "occurredAt":"2026-11-27T00:00:00Z",
                 "payload":{"orderId":%d,"customerId":1,"productId":%d,"quantity":3,"totalAmount":15000}}
                """.formatted(eventId, orderId, orderId, productId);
    }

    @Autowired
    OrderEventConsumer orderEventConsumer;

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    ProcessedEventRepository processedEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    List<OutboxEvent> outboxOf(long orderId) {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(String.valueOf(orderId))).toList();
    }

    @Test
    void 같은_이벤트가_두_번_도착해도_재고는_한_번만_차감된다() {
        stockRepository.save(new Stock(500L, 10));

        orderEventConsumer.onOrderEvent(orderMessage("dup-1", 501L, 500L));
        orderEventConsumer.onOrderEvent(orderMessage("dup-1", 501L, 500L)); // 같은 메시지 재전달

        assertThat(stockRepository.findById(500L).orElseThrow().getQuantity()).isEqualTo(7); // 10 - 3
    }

    @Test
    void 같은_이벤트가_두_번_도착해도_STOCK_RESERVED는_한_번만_기록된다() {
        stockRepository.save(new Stock(510L, 10));

        orderEventConsumer.onOrderEvent(orderMessage("dup-2", 511L, 510L));
        orderEventConsumer.onOrderEvent(orderMessage("dup-2", 511L, 510L));

        assertThat(outboxOf(511L)).extracting(OutboxEvent::getEventType).containsExactly("STOCK_RESERVED");
    }

    @Test
    void 내용이_같아도_eventId가_다르면_별개의_이벤트로_처리한다() {
        stockRepository.save(new Stock(520L, 10));

        orderEventConsumer.onOrderEvent(orderMessage("dup-3a", 521L, 520L));
        orderEventConsumer.onOrderEvent(orderMessage("dup-3b", 522L, 520L));

        assertThat(stockRepository.findById(520L).orElseThrow().getQuantity()).isEqualTo(4); // 10 - 3 - 3
    }

    @Test
    void 같은_결제_실패_이벤트가_두_번_와도_재고는_한_번만_복구된다() {
        stockRepository.save(new Stock(530L, 10));
        stockService.reserve("dup-4-order", new OrderCreatedPayload(531L, 1L, 530L, 3, 15_000L)); // 10 -> 7
        PaymentFailedPayload failed = new PaymentFailedPayload(531L, 530L, 3, "결제 한도 초과");

        stockService.release("dup-4-pay", failed);
        stockService.release("dup-4-pay", failed); // 재전달

        assertThat(stockRepository.findById(530L).orElseThrow().getQuantity()).isEqualTo(10); // 13이 아니다
        assertThat(outboxOf(531L)).extracting(OutboxEvent::getEventType)
                .containsExactlyInAnyOrder("STOCK_RESERVED", "STOCK_RELEASED");
    }

    @Test
    void 처리_중_롤백되면_처리_기록도_함께_사라져서_재전달_때_다시_처리된다() {
        stockRepository.save(new Stock(540L, 10));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            stockService.reserve("dup-5", new OrderCreatedPayload(541L, 1L, 540L, 3, 15_000L));
            throw new IllegalStateException("커밋 직전 장애");
        })).isInstanceOf(IllegalStateException.class);
        // 재고도, 이벤트도, "처리했다"는 기록도 모두 없다.
        assertThat(stockRepository.findById(540L).orElseThrow().getQuantity()).isEqualTo(10);
        assertThat(processedEventRepository.existsById("dup-5")).isFalse();

        // 메시지가 재전달되면 이번에는 정상 처리된다. (기록만 남고 처리가 안 되는 일이 없다)
        stockService.reserve("dup-5", new OrderCreatedPayload(541L, 1L, 540L, 3, 15_000L));
        assertThat(stockRepository.findById(540L).orElseThrow().getQuantity()).isEqualTo(7);
    }

    @Test
    void 같은_이벤트가_동시에_두_번_들어와도_재고는_한_번만_차감된다() throws Exception {
        stockRepository.save(new Stock(550L, 10));
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    stockService.reserve("dup-6", new OrderCreatedPayload(551L, 1L, 550L, 3, 15_000L));
                    return null;
                }));
            }
            start.countDown();
            // 늦게 들어온 쪽은 건너뛰거나(이미 커밋됨) PK 위반으로 롤백된다. 어느 쪽이든 효과는 한 번뿐이어야 한다.
            for (Future<?> future : futures) {
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (ExecutionException ignored) {
                    // PK 위반으로 실패한 쪽. 메시지는 재전달되어 다음에 건너뛰게 된다.
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(stockRepository.findById(550L).orElseThrow().getQuantity()).isEqualTo(7);
        assertThat(outboxOf(551L)).extracting(OutboxEvent::getEventType).containsExactly("STOCK_RESERVED");
    }
}
