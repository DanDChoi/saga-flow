package com.dan.sagaflow.order.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.OrderCreatedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.InvalidOrderStatusTransitionException;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.outbox.OutboxWriter;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;

    public OrderService(OrderRepository orderRepository, OutboxWriter outboxWriter) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        long totalAmount = Math.multiplyExact(request.unitPrice(), (long) request.quantity());
        Order order = Order.create(request.customerId(), request.productId(), request.quantity(), totalAmount);
        Order saved = orderRepository.save(order);
        // Kafka로 바로 보내지 않고 같은 트랜잭션에서 outbox에 기록한다. 실제 발행은 릴레이가 맡는다. (Day 25)
        String key = String.valueOf(saved.getId()); // 같은 주문의 이벤트는 같은 파티션 -> 순서 보장
        outboxWriter.append(Topics.ORDER_EVENTS, EventEnvelope.of(OrderCreatedPayload.EVENT_TYPE, key,
                new OrderCreatedPayload(saved.getId(), saved.getCustomerId(), saved.getProductId(),
                        saved.getQuantity(), saved.getTotalAmount())));
        return saved;
    }

    @Transactional
    public void completeOrder(Long orderId) {
        changeStatus(orderId, Order::complete);
    }

    @Transactional
    public void cancelOrder(Long orderId) {
        changeStatus(orderId, Order::cancel);
    }

    /**
     * 늦게 도착했거나 중복된 이벤트로 허용되지 않는 전이가 요청될 수 있다.
     * 이는 재시도해도 결과가 같은 일이므로 예외를 밖으로 던지지 않고 경고만 남긴다.
     */
    private void changeStatus(Long orderId, Consumer<Order> change) {
        orderRepository.findById(orderId).ifPresentOrElse(order -> {
            try {
                change.accept(order);
            } catch (InvalidOrderStatusTransitionException e) {
                log.warn("주문 상태 전이 무시: {}", e.getMessage());
            }
        }, () -> log.warn("상태를 바꿀 주문이 없음 orderId={}", orderId));
    }
}
