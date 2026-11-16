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

/**
 * 보상 흐름: 어느 단계에서 실패하든 주문은 CANCELLED가 되고, 이미 차감한 재고는 원상 복구된다.
 * 테스트끼리 간섭하지 않도록 상품 ID를 다르게 쓴다.
 */
class CompensationIntegrationTest extends SagaFlowIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void 결제가_실패하면_주문이_취소되고_차감했던_재고가_복구된다() throws Exception {
        long productId = 200L;
        StockRepository stockRepository = inventoryContext.getBean(StockRepository.class);
        stockRepository.save(new Stock(productId, 10));

        long orderId = placeOrder(1L, productId, 2, 600_000L); // 총액 1,200,000원 (결제 한도 초과)

        OrderRepository orderRepository = orderContext.getBean(OrderRepository.class);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(orderRepository.findById(orderId).orElseThrow().getStatus())
                        .isEqualTo(OrderStatus.CANCELLED));

        // 주문이 취소됐다는 건 재고 차감 후 결제까지 실패했다는 뜻이다. 복구(보상)는 그 뒤에 따로 도착한다.
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(stockRepository.findById(productId).orElseThrow().getQuantity()).isEqualTo(10));

        List<Payment> payments = paymentsOf(orderId);
        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payments.get(0).getFailureReason()).contains("한도 초과");

        assertThat(awaitNotificationTypes(orderId, 1)).containsExactly("PAYMENT_FAILED");
    }

    @Test
    void 재고가_부족하면_결제_없이_주문이_취소되고_재고는_그대로다() throws Exception {
        long productId = 300L;
        StockRepository stockRepository = inventoryContext.getBean(StockRepository.class);
        stockRepository.save(new Stock(productId, 1));

        long orderId = placeOrder(1L, productId, 5, 100L);

        OrderRepository orderRepository = orderContext.getBean(OrderRepository.class);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(orderRepository.findById(orderId).orElseThrow().getStatus())
                        .isEqualTo(OrderStatus.CANCELLED));

        assertThat(awaitNotificationTypes(orderId, 1)).containsExactly("STOCK_FAILED");
        assertThat(stockRepository.findById(productId).orElseThrow().getQuantity()).isEqualTo(1);
        assertThat(paymentsOf(orderId)).isEmpty(); // STOCK_RESERVED가 없었으니 결제 시도 자체가 없다.
    }

    private List<Payment> paymentsOf(long orderId) {
        return paymentContext.getBean(PaymentRepository.class).findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId)).toList();
    }

    /** 알림은 주문 상태 갱신과 별개로 소비되므로 기다렸다가 expectedCount건이 되면 이벤트 종류를 돌려준다. */
    private List<String> awaitNotificationTypes(long orderId, int expectedCount) {
        NotificationRepository repository = notificationContext.getBean(NotificationRepository.class);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(repository.findByOrderIdOrderByIdAsc(orderId)).hasSize(expectedCount));
        return repository.findByOrderIdOrderByIdAsc(orderId).stream().map(Notification::getEventType).toList();
    }
}
