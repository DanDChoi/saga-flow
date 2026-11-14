package com.dan.sagaflow.order.application;

import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.domain.InvalidOrderStatusTransitionException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;

    private final OrderEventPublisher eventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        long totalAmount = Math.multiplyExact(request.unitPrice(), (long) request.quantity());
        Order order = Order.create(request.customerId(), request.productId(), request.quantity(), totalAmount);
        Order saved = orderRepository.save(order);
        // 주의: 아직 트랜잭션 안이라, 발행 후 커밋이 실패하면 없는 주문의 이벤트가 나간다. (Outbox로 해결 예정)
        eventPublisher.publishOrderCreated(saved);
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
