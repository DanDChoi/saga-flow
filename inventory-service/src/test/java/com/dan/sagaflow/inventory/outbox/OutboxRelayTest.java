package com.dan.sagaflow.inventory.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@DataJpaTest
@ActiveProfiles("test")
@SuppressWarnings("unchecked")
class OutboxRelayTest {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    @Autowired
    PlatformTransactionManager transactionManager;

    OutboxRelay relay() {
        return relay(100);
    }

    OutboxRelay relay(int batchSize) {
        return new OutboxRelay(outboxEventRepository, kafkaTemplate, new TransactionTemplate(transactionManager), batchSize);
    }

    OutboxEvent save(String eventId, String aggregateId) {
        return outboxEventRepository.saveAndFlush(
                new OutboxEvent(eventId, "inventory-events", aggregateId, "STOCK_RESERVED", "{\"eventId\":\"" + eventId + "\"}"));
    }

    CompletableFuture<SendResult<String, String>> ok() {
        return CompletableFuture.completedFuture(mock(SendResult.class, RETURNS_DEEP_STUBS));
    }

    @Test
    void 미발행_행을_오래된_순서대로_그대로_발행하고_발행_완료로_표시한다() {
        OutboxEvent first = save("e-1", "1");
        OutboxEvent second = save("e-2", "2");
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(ok());

        relay().relay();

        InOrder inOrder = inOrder(kafkaTemplate);
        inOrder.verify(kafkaTemplate).send("inventory-events", "1", "{\"eventId\":\"e-1\"}");
        inOrder.verify(kafkaTemplate).send("inventory-events", "2", "{\"eventId\":\"e-2\"}");
        assertThat(first.isPublished()).isTrue();
        assertThat(second.isPublished()).isTrue();
    }

    @Test
    void 이미_발행된_행은_다시_보내지_않는다() {
        OutboxEvent done = save("e-1", "1");
        done.markPublished();
        outboxEventRepository.flush();

        relay().relay();

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void 발행에_실패하면_그_행은_미발행으로_남고_뒤의_행도_보내지_않는다() {
        OutboxEvent first = save("e-1", "1");
        OutboxEvent second = save("e-2", "2");
        when(kafkaTemplate.send(eq("inventory-events"), eq("1"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 연결 실패")));

        relay().relay();

        assertThat(first.isPublished()).isFalse();
        assertThat(second.isPublished()).isFalse();
        verify(kafkaTemplate, never()).send(eq("inventory-events"), eq("2"), anyString());
    }

    @Test
    void 실패했던_행은_다음_주기에_다시_발행된다() {
        OutboxEvent event = save("e-1", "1");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 연결 실패")))
                .thenReturn(ok());

        relay().relay();
        assertThat(event.isPublished()).isFalse();

        relay().relay();
        assertThat(event.isPublished()).isTrue();
    }

    @Test
    void 발행할_게_없으면_아무것도_보내지_않는다() {
        relay().relay();

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void 배치보다_많이_쌓여_있으면_다음_주기를_기다리지_않고_이어서_모두_발행한다() {
        OutboxEvent[] events = {save("e-1", "1"), save("e-2", "2"), save("e-3", "3"), save("e-4", "4"), save("e-5", "5")};
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(ok());

        relay(2).relay(); // 배치 크기 2 -> 2 + 2 + 1개로 나눠 한 번의 호출에서 모두 처리

        assertThat(events).allMatch(OutboxEvent::isPublished);
        verify(kafkaTemplate, times(5)).send(anyString(), anyString(), anyString());
    }

    @Test
    void 중간_배치에서_실패하면_거기서_멈춘다() {
        OutboxEvent[] events = {save("e-1", "1"), save("e-2", "2"), save("e-3", "3")};
        when(kafkaTemplate.send(eq("inventory-events"), eq("1"), anyString())).thenReturn(ok());
        when(kafkaTemplate.send(eq("inventory-events"), eq("2"), anyString())).thenReturn(ok());
        when(kafkaTemplate.send(eq("inventory-events"), eq("3"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("브로커 연결 실패")));

        relay(2).relay();

        assertThat(events[0].isPublished()).isTrue();
        assertThat(events[1].isPublished()).isTrue();
        assertThat(events[2].isPublished()).isFalse();
    }
}
