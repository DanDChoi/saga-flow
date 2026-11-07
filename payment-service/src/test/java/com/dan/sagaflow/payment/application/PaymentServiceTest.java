package com.dan.sagaflow.payment.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentRepository;
import com.dan.sagaflow.payment.domain.PaymentStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class PaymentServiceTest {

    @Autowired
    PaymentService paymentService;

    @Autowired
    PaymentRepository paymentRepository;

    @Test
    void 결제에_성공하면_COMPLETED로_저장된다() {
        Payment payment = paymentService.pay(1L, 15_000L);

        Payment found = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(found.getOrderId()).isEqualTo(1L);
        assertThat(found.getAmount()).isEqualTo(15_000L);
        assertThat(found.getFailureReason()).isNull();
        assertThat(found.getCreatedAt()).isNotNull();
    }

    @Test
    void 결제가_거절되면_사유와_함께_FAILED로_저장된다() {
        Payment payment = paymentService.pay(2L, 5_000_000L);

        Payment found = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(found.getFailureReason()).contains("한도 초과");
    }
}
