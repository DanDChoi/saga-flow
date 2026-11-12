package com.dan.sagaflow.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StockTest {

    @Test
    void 재고를_차감한다() {
        Stock stock = new Stock(100L, 10);

        stock.decrease(3);

        assertThat(stock.getQuantity()).isEqualTo(7);
    }

    @Test
    void 남은_재고와_같은_수량까지는_차감할_수_있다() {
        Stock stock = new Stock(100L, 3);

        stock.decrease(3);

        assertThat(stock.getQuantity()).isZero();
    }

    @Test
    void 재고보다_많이_차감하면_예외가_나고_재고는_그대로다() {
        Stock stock = new Stock(100L, 2);

        assertThatThrownBy(() -> stock.decrease(3)).isInstanceOf(InsufficientStockException.class);
        assertThat(stock.getQuantity()).isEqualTo(2);
    }

    @Test
    void 재고를_되돌린다() {
        Stock stock = new Stock(100L, 7);

        stock.increase(3);

        assertThat(stock.getQuantity()).isEqualTo(10);
    }

    @Test
    void 복구_수량은_1_이상이어야_한다() {
        Stock stock = new Stock(100L, 7);

        assertThatThrownBy(() -> stock.increase(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 차감_수량은_1_이상이어야_한다() {
        Stock stock = new Stock(100L, 2);

        assertThatThrownBy(() -> stock.decrease(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
