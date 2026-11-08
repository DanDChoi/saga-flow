package com.dan.sagaflow.payment.application;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FakePaymentGatewayTest {

    FakePaymentGateway gateway = new FakePaymentGateway();

    @Test
    void 한도_이하면_승인한다() {
        assertThat(gateway.charge(1L, 1_000_000L).success()).isTrue();
    }

    @Test
    void 한도를_넘으면_사유와_함께_거절한다() {
        PaymentGateway.PaymentResult result = gateway.charge(1L, 1_000_001L);

        assertThat(result.success()).isFalse();
        assertThat(result.failureReason()).contains("한도 초과");
    }
}
