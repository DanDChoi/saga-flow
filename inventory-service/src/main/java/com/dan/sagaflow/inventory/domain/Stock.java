package com.dan.sagaflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** 상품별 재고. 상품 하나당 한 행이므로 productId를 PK로 쓴다. */
@Entity
@Table(name = "stock")
public class Stock {

    @Id
    private Long productId;

    @Column(nullable = false)
    private int quantity;

    protected Stock() {
    }

    public Stock(Long productId, int quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    /** 재고를 차감한다. 남은 재고보다 많이 요구하면 아무것도 바꾸지 않고 예외를 던진다. */
    public void decrease(int amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("차감 수량은 1 이상이어야 합니다: " + amount);
        }
        if (quantity < amount) {
            throw new InsufficientStockException(productId, amount, quantity);
        }
        this.quantity -= amount;
    }

    public Long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }
}
