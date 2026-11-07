package com.dan.sagaflow.payment.application;

import org.springframework.stereotype.Component;

/**
 * 가짜 PG. 항상 같은 입력에 같은 결과를 내야 테스트와 수동 검증이 쉬우므로 규칙은 결정적이다:
 * 한도(1,000,000원)를 넘는 금액은 거절하고, 나머지는 승인한다.
 */
@Component
public class FakePaymentGateway implements PaymentGateway {

    static final long LIMIT = 1_000_000L;

    @Override
    public PaymentResult charge(Long orderId, long amount) {
        if (amount > LIMIT) {
            return PaymentResult.declined("결제 한도 초과 amount=" + amount);
        }
        return PaymentResult.approved();
    }
}
