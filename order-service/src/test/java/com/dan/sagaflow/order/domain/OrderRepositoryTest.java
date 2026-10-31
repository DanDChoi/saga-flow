package com.dan.sagaflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

@DataJpaTest
@ActiveProfiles("test")
class OrderRepositoryTest {

    @Autowired
    OrderRepository orderRepository;

    @Test
    void 주문을_저장하면_CREATED_상태로_ID와_시각이_채워진다() {
        Order saved = orderRepository.saveAndFlush(Order.create(1L, 100L, 2, 20_000L));

        Order found = orderRepository.findById(saved.getId()).orElseThrow();

        assertThat(found.getId()).isNotNull();
        assertThat(found.getStatus()).isEqualTo(OrderStatus.CREATED);
        assertThat(found.getCustomerId()).isEqualTo(1L);
        assertThat(found.getProductId()).isEqualTo(100L);
        assertThat(found.getQuantity()).isEqualTo(2);
        assertThat(found.getTotalAmount()).isEqualTo(20_000L);
        assertThat(found.getCreatedAt()).isNotNull();
        assertThat(found.getUpdatedAt()).isNotNull();
    }
}
