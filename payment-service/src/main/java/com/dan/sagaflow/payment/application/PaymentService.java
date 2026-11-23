package com.dan.sagaflow.payment.application;

import com.dan.sagaflow.common.event.EventEnvelope;
import com.dan.sagaflow.common.event.PaymentCompletedPayload;
import com.dan.sagaflow.common.event.PaymentFailedPayload;
import com.dan.sagaflow.common.event.StockReservedPayload;
import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.payment.domain.Payment;
import com.dan.sagaflow.payment.domain.PaymentRepository;
import com.dan.sagaflow.payment.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 결제 결과 저장과 그 결과 이벤트(outbox 기록)를 한 트랜잭션으로 처리한다.
 * 결제 기록만 남고 이벤트가 사라지거나, 이벤트만 나가고 결제 기록이 없는 일이 없다.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentGateway paymentGateway;
    private final OutboxWriter outboxWriter;

    public PaymentService(PaymentRepository paymentRepository, PaymentGateway paymentGateway,
                          OutboxWriter outboxWriter) {
        this.paymentRepository = paymentRepository;
        this.paymentGateway = paymentGateway;
        this.outboxWriter = outboxWriter;
    }

    /**
     * 결제를 시도하고 결과(성공/실패)를 Payment로 저장하면서 PAYMENT_COMPLETED 또는 PAYMENT_FAILED를 기록한다.
     * 거절은 예외가 아니라 FAILED 결과다. (예외를 올리면 롤백되어 실패 이벤트도 사라진다)
     * PAYMENT_FAILED에는 재고 복구에 필요한 productId, quantity를 STOCK_RESERVED에서 받아 그대로 싣는다.
     */
    @Transactional
    public Payment pay(StockReservedPayload reserved) {
        Long orderId = reserved.orderId();
        PaymentGateway.PaymentResult result = paymentGateway.charge(orderId, reserved.totalAmount());

        if (result.success()) {
            Payment payment = paymentRepository.save(Payment.completed(orderId, reserved.totalAmount()));
            outboxWriter.append(Topics.PAYMENT_EVENTS, EventEnvelope.of(PaymentCompletedPayload.EVENT_TYPE,
                    String.valueOf(orderId),
                    new PaymentCompletedPayload(orderId, payment.getId(), payment.getAmount())));
            return payment;
        }

        log.warn("결제 실패 orderId={} reason={}", orderId, result.failureReason());
        Payment payment = paymentRepository.save(Payment.failed(orderId, reserved.totalAmount(), result.failureReason()));
        outboxWriter.append(Topics.PAYMENT_EVENTS, EventEnvelope.of(PaymentFailedPayload.EVENT_TYPE,
                String.valueOf(orderId),
                new PaymentFailedPayload(orderId, reserved.productId(), reserved.quantity(), result.failureReason())));
        return payment;
    }
}
