package com.dan.sagaflow.order.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateOrderRequest(
        @NotNull Long customerId,
        @NotNull Long productId,
        @Min(1) int quantity,
        @Positive long unitPrice
) {
}
