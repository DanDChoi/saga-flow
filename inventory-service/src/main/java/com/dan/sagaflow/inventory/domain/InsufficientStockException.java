package com.dan.sagaflow.inventory.domain;

/** 재고가 요구 수량보다 부족할 때 (재고 행이 없는 상품은 available = 0). */
public class InsufficientStockException extends RuntimeException {

    public InsufficientStockException(Long productId, int requested, int available) {
        super("재고 부족 productId=%d requested=%d available=%d".formatted(productId, requested, available));
    }
}
