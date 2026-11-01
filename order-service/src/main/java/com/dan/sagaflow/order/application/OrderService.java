package com.dan.sagaflow.order.application;

import com.dan.sagaflow.order.api.CreateOrderRequest;
import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        long totalAmount = Math.multiplyExact(request.unitPrice(), (long) request.quantity());
        Order order = Order.create(request.customerId(), request.productId(), request.quantity(), totalAmount);
        return orderRepository.save(order);
    }
}
