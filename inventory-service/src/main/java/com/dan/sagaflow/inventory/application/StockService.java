package com.dan.sagaflow.inventory.application;

import com.dan.sagaflow.inventory.domain.InsufficientStockException;
import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StockService {

    private final StockRepository stockRepository;

    public StockService(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    /**
     * 재고를 차감한다. 재고가 부족하거나 상품이 없으면 InsufficientStockException.
     * 예외가 나면 트랜잭션이 롤백되므로 재고는 변하지 않는다.
     */
    @Transactional
    public void decrease(Long productId, int quantity) {
        Stock stock = stockRepository.findByProductIdForUpdate(productId)
                .orElseThrow(() -> new InsufficientStockException(productId, quantity, 0));
        stock.decrease(quantity);
    }

    /** 차감했던 재고를 되돌린다. 차감된 적이 있는 상품이어야 하므로 재고 행이 없으면 상태가 잘못된 것이다. */
    @Transactional
    public void increase(Long productId, int quantity) {
        Stock stock = stockRepository.findByProductIdForUpdate(productId)
                .orElseThrow(() -> new IllegalStateException("복구할 재고 행이 없음 productId=" + productId));
        stock.increase(quantity);
    }
}
