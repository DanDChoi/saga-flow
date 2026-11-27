package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.StockFailedPayload;
import com.dan.sagaflow.common.event.StockReleasedPayload;
import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.inventory.domain.InsufficientStockException;
import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import com.dan.sagaflow.inventory.idempotency.ProcessedEvent;
import com.dan.sagaflow.inventory.idempotency.ProcessedEventRepository;
import com.dan.sagaflow.inventory.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 변경과 그 결과 이벤트(outbox 기록)를 한 트랜잭션으로 처리한다.
 * 재고만 바뀌고 이벤트가 사라지거나, 이벤트만 나가고 재고가 안 바뀌는 일이 없다.
 */
@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final StockRepository stockRepository;
    private final OutboxWriter outboxWriter;
    private final ProcessedEventRepository processedEventRepository;

    public StockService(StockRepository stockRepository, OutboxWriter outboxWriter,
                        ProcessedEventRepository processedEventRepository) {
        this.stockRepository = stockRepository;
        this.outboxWriter = outboxWriter;
        this.processedEventRepository = processedEventRepository;
    }

    /**
     * 주문 수량만큼 재고를 확보(차감)하고 STOCK_RESERVED를 기록한다.
     * 재고가 부족하거나 상품이 없으면 재고는 그대로 두고 STOCK_FAILED를 기록한다.
     * 재고 부족은 예상 가능한 정상 결과라 예외로 올리지 않는다. (예외를 올리면 트랜잭션이 롤백되어 이벤트도 사라진다)
     *
     * <p>멱등: 이미 처리한 eventId(= 중복 전달된 메시지)면 아무것도 하지 않는다.
     */
    @Transactional
    public void reserve(String eventId, OrderCreatedPayload order) {
        if (alreadyProcessed(eventId, "ORDER_CREATED")) {
            return;
        }
        String key = String.valueOf(order.orderId());
        try {
            Stock stock = stockRepository.findByProductIdForUpdate(order.productId())
                    .orElseThrow(() -> new InsufficientStockException(order.productId(), order.quantity(), 0));
            stock.decrease(order.quantity());
            outboxWriter.append(Topics.INVENTORY_EVENTS, EventEnvelope.of(StockReservedPayload.EVENT_TYPE, key,
                    new StockReservedPayload(order.orderId(), order.productId(), order.quantity(), order.totalAmount())));
            log.info("재고 차감 완료 orderId={}", order.orderId());
        } catch (InsufficientStockException e) {
            log.warn("재고 차감 실패 orderId={} reason={}", order.orderId(), e.getMessage());
            outboxWriter.append(Topics.INVENTORY_EVENTS, EventEnvelope.of(StockFailedPayload.EVENT_TYPE, key,
                    new StockFailedPayload(order.orderId(), order.productId(), e.getMessage())));
        }
    }

    /**
     * 결제 실패로 차감했던 재고를 되돌리고(보상) STOCK_RELEASED를 기록한다.
     * 차감된 적이 있는 상품이어야 하므로 재고 행이 없으면 상태가 잘못된 것이라 예외를 던진다.
     *
     * <p>멱등: 이미 처리한 eventId면 아무것도 하지 않는다. (재고가 두 번 복구되면 없던 재고가 생긴다)
     */
    @Transactional
    public void release(String eventId, PaymentFailedPayload failed) {
        if (alreadyProcessed(eventId, PaymentFailedPayload.EVENT_TYPE)) {
            return;
        }
        Stock stock = stockRepository.findByProductIdForUpdate(failed.productId())
                .orElseThrow(() -> new IllegalStateException("복구할 재고 행이 없음 productId=" + failed.productId()));
        stock.increase(failed.quantity());
        outboxWriter.append(Topics.INVENTORY_EVENTS, EventEnvelope.of(StockReleasedPayload.EVENT_TYPE,
                String.valueOf(failed.orderId()),
                new StockReleasedPayload(failed.orderId(), failed.productId(), failed.quantity())));
        log.info("결제 실패로 재고 복구 orderId={} productId={} quantity={}",
                failed.orderId(), failed.productId(), failed.quantity());
    }

    /**
     * 이 eventId를 처리했다고 기록하고, 이미 기록돼 있었으면 true를 돌려준다.
     *
     * <p>기록은 호출자의 트랜잭션에 참여하므로 재고 변경과 함께 커밋/롤백된다.
     * 거의 동시에 같은 이벤트가 두 번 들어오면 둘 다 "없음"으로 볼 수 있지만, PK 제약 때문에
     * 한쪽의 insert는 실패하고 그 트랜잭션(재고 변경 포함)은 통째로 롤백된다. 재전달 때 건너뛰게 된다.
     */
    private boolean alreadyProcessed(String eventId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            log.info("이미 처리한 이벤트라 건너뜀 eventId={} type={}", eventId, eventType);
            return true;
        }
        processedEventRepository.saveAndFlush(new ProcessedEvent(eventId, eventType));
        return false;
    }
}
