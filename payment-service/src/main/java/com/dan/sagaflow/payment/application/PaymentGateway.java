package com.dan.sagaflow.payment.application;

/** 외부 결제 시스템(PG)에 대한 경계. 실제 연동 대신 가짜 구현을 꽂아 쓴다. */
public interface PaymentGateway {

    PaymentResult charge(Long orderId, long amount);

    record PaymentResult(boolean success, String failureReason) {

        public static PaymentResult approved() {
            return new PaymentResult(true, null);
        }

        public static PaymentResult declined(String reason) {
            return new PaymentResult(false, reason);
        }
    }
}
