package com.dan.sagaflow.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 릴레이 인스턴스가 둘 이상 동시에 돌아도 같은 outbox 행을 중복 발행하지 않는지 확인한다.
 *
 * <p>"중복이 없다"는 결과만 본다. 행 잠금 자체는 SKIP LOCKED가 아니어도 이 결과를 만들 수 있으므로
 * (잠긴 행을 기다리면 직렬로 처리될 뿐) 잠긴 행을 건너뛰는지는 OutboxSkipLockedTest가 따로 확인한다.
 */
@SpringBootTest
@ActiveProfiles("test")
@SuppressWarnings({"unchecked", "rawtypes"})
class OutboxRelayConcurrencyTest {

    static final int EVENT_COUNT = 30;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @AfterEach
    void clean() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void 릴레이_두_개가_동시에_돌아도_모든_이벤트는_정확히_한_번씩만_발행된다() throws Exception {
        for (int i = 1; i <= EVENT_COUNT; i++) {
            outboxEventRepository.save(new OutboxEvent("e-" + i, "order-events", String.valueOf(i), "ORDER_CREATED",
                    "{\"n\":" + i + "}"));
        }

        List<String> sentPayloads = new CopyOnWriteArrayList<>();
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            Thread.sleep(5); // 발행이 느려서 두 릴레이의 배치가 겹치도록 한다.
            sentPayloads.add(invocation.getArgument(2));
            return CompletableFuture.completedFuture(mock(SendResult.class, RETURNS_DEEP_STUBS));
        });

        // 배치를 작게(3) 잡아 여러 번 번갈아 가져가게 한다.
        OutboxRelay relayA = new OutboxRelay(outboxEventRepository, kafkaTemplate, transactionTemplate, 3);
        OutboxRelay relayB = new OutboxRelay(outboxEventRepository, kafkaTemplate, transactionTemplate, 3);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> a = executor.submit(relayA::relay);
            Future<?> b = executor.submit(relayB::relay);
            a.get(30, TimeUnit.SECONDS);
            b.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(sentPayloads).hasSize(EVENT_COUNT).doesNotHaveDuplicates();
        assertThat(outboxEventRepository.findAll()).hasSize(EVENT_COUNT).allMatch(OutboxEvent::isPublished);
    }
}
