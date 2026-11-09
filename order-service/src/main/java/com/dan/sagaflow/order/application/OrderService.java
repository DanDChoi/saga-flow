package com.dan.sagaflow.order.application;

import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
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
        orderRepository.findById(orderId).ifPresentOrElse(Order::complete,
                () -> log.warn("완료 처리할 주문이 없음 orderId={}", orderId));
    }

    @Transactional
    public void cancelOrder(Long orderId) {
        orderRepository.findById(orderId).ifPresentOrElse(Order::cancel,
                () -> log.warn("취소 처리할 주문이 없음 orderId={}", orderId));
    }
}
