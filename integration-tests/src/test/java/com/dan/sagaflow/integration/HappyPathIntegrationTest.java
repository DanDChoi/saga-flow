package com.dan.sagaflow.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dan.sagaflow.inventory.domain.Stock;
import com.dan.sagaflow.inventory.domain.StockRepository;
import com.dan.sagaflow.notification.domain.Notification;
import com.dan.sagaflow.notification.domain.NotificationRepository;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.domain.OrderStatus;
import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentRepository;
import com.dan.sagaflow.payment.domain.PaymentStatus;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 정상 흐름: 재고 확보 → 결제 성공 → 주문 COMPLETED. 모든 서비스가 진짜 Kafka로 이벤트를 주고받는다. */
class HappyPathIntegrationTest extends SagaFlowIntegrationTest {

    static final long PRODUCT_ID = 100L;

    @Test
    void 주문하면_재고가_차감되고_결제되고_주문이_COMPLETED가_된다() throws Exception {
        StockRepository stockRepository = inventoryContext.getBean(StockRepository.class);
        stockRepository.save(new Stock(PRODUCT_ID, 10));

        long orderId = placeOrder(1L, PRODUCT_ID, 3, 5_000L); // 총액 15,000원 (결제 한도 이하)

        OrderRepository orderRepository = orderContext.getBean(OrderRepository.class);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orderRepository.findById(orderId).orElseThrow().getStatus())
                        .isEqualTo(OrderStatus.COMPLETED));

        assertThat(stockRepository.findById(PRODUCT_ID).orElseThrow().getQuantity()).isEqualTo(7);

        List<Payment> payments = paymentContext.getBean(PaymentRepository.class).findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId)).toList();
        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(payments.get(0).getAmount()).isEqualTo(15_000L);

        // 알림은 결제 완료 이벤트를 따로 소비하므로 주문 완료보다 조금 늦을 수 있다.
        NotificationRepository notificationRepository = notificationContext.getBean(NotificationRepository.class);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<Notification> notifications = notificationRepository.findByOrderIdOrderByIdAsc(orderId);
            assertThat(notifications).extracting(Notification::getEventType).containsExactly("PAYMENT_COMPLETED");
        });
    }
}
