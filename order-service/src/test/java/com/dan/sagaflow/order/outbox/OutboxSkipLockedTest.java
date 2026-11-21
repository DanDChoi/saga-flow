package com.dan.sagaflow.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * SKIP LOCKED 동작 확인. 실제로 두 개의 트랜잭션(= 두 릴레이 인스턴스)이 동시에 열려 있어야 하므로
 * 테스트 자체는 트랜잭션 없이 실행하고, 각 트랜잭션을 TransactionTemplate으로 직접 연다.
 */
@SpringBootTest
@ActiveProfiles("test")
class OutboxSkipLockedTest {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @AfterEach
    void clean() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void 다른_트랜잭션이_잠근_행은_기다리지_않고_건너뛰어_서로_다른_행을_가져간다() throws Exception {
        for (int i = 1; i <= 4; i++) {
            outboxEventRepository.save(new OutboxEvent("e-" + i, "order-events", String.valueOf(i), "ORDER_CREATED", "{}"));
        }

        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        // 인스턴스 A: 앞의 2개를 잠그고 트랜잭션을 끝내지 않은 채 붙들고 있다.
        CompletableFuture<List<String>> instanceA = CompletableFuture.supplyAsync(() ->
                transactionTemplate.execute(status -> {
                    List<String> ids = eventIds(outboxEventRepository.findPendingForUpdate(2));
                    firstLocked.countDown();
                    await(releaseFirst);
                    return ids;
                }));

        assertThat(firstLocked.await(10, TimeUnit.SECONDS)).isTrue();

        // 인스턴스 B: A가 잠근 행을 기다리지 않고 건너뛰어 나머지 행을 가져온다.
        List<String> instanceB = transactionTemplate.execute(status ->
                eventIds(outboxEventRepository.findPendingForUpdate(2)));

        releaseFirst.countDown();
        List<String> a = instanceA.get(10, TimeUnit.SECONDS);

        assertThat(a).containsExactly("e-1", "e-2");
        assertThat(instanceB).containsExactly("e-3", "e-4");
    }

    private static List<String> eventIds(List<OutboxEvent> events) {
        return events.stream().map(OutboxEvent::getEventId).toList();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
