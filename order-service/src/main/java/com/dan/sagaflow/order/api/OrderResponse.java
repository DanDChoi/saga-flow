package com.dan.sagaflow.order.api;

import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderStatus;
import java.time.LocalDateTime;

public record OrderResponse(
        Long orderId,
        Long customerId,
        Long productId,
        int quantity,
        long totalAmount,
        OrderStatus status,
        LocalDateTime createdAt
) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalAmount(),
                order.getStatus(),
                order.getCreatedAt()
        );
    }
}
