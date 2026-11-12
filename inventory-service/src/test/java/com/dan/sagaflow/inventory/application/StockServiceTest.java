package com.dan.sagaflow.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dan.sagaflow.inventory.domain.InsufficientStockException;
import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class StockServiceTest {

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    @Test
    void 재고를_차감하면_DB에_반영된다() {
        stockRepository.save(new Stock(1L, 10));

        stockService.decrease(1L, 4);

        assertThat(stockRepository.findById(1L).orElseThrow().getQuantity()).isEqualTo(6);
    }

    @Test
    void 차감했던_재고를_되돌리면_DB에_반영된다() {
        stockRepository.save(new Stock(3L, 10));
        stockService.decrease(3L, 4);

        stockService.increase(3L, 4);

        assertThat(stockRepository.findById(3L).orElseThrow().getQuantity()).isEqualTo(10);
    }

    @Test
    void 재고_행이_없는_상품은_복구할_수_없다() {
        assertThatThrownBy(() -> stockService.increase(998L, 1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void 재고가_부족하면_예외가_나고_DB는_그대로다() {
        stockRepository.save(new Stock(2L, 1));

        assertThatThrownBy(() -> stockService.decrease(2L, 2)).isInstanceOf(InsufficientStockException.class);

        assertThat(stockRepository.findById(2L).orElseThrow().getQuantity()).isEqualTo(1);
    }

    @Test
    void 재고_행이_없는_상품도_재고_부족으로_취급한다() {
        assertThatThrownBy(() -> stockService.decrease(999L, 1)).isInstanceOf(InsufficientStockException.class);
    }
}
