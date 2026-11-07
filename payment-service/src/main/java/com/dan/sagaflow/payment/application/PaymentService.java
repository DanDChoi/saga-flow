package com.dan.sagaflow.payment.application;

import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentGateway paymentGateway;

    public PaymentService(PaymentRepository paymentRepository, PaymentGateway paymentGateway) {
        this.paymentRepository = paymentRepository;
        this.paymentGateway = paymentGateway;
    }

    /** 결제를 시도하고 결과(성공/실패)를 Payment로 저장해 돌려준다. 거절은 예외가 아니라 FAILED 결과다. */
    @Transactional
    public Payment pay(Long orderId, long amount) {
        PaymentGateway.PaymentResult result = paymentGateway.charge(orderId, amount);
        Payment payment = result.success()
                ? Payment.completed(orderId, amount)
                : Payment.failed(orderId, amount, result.failureReason());
        return paymentRepository.save(payment);
    }
}
