package com.dan.sagaflow.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

/** 결제 시도 결과. 주문 하나에 결제 한 건이다. */
@Entity
@Table(name = "payment")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long orderId;

    /** 결제 금액 (원 단위) */
    @Column(nullable = false)
    private long amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    /** 실패했을 때만 채워진다. */
    private String failureReason;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    protected Payment() {
    }

    private Payment(Long orderId, long amount, PaymentStatus status, String failureReason) {
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.failureReason = failureReason;
    }

    public static Payment completed(Long orderId, long amount) {
        return new Payment(orderId, amount, PaymentStatus.COMPLETED, null);
    }

    public static Payment failed(Long orderId, long amount, String reason) {
        return new Payment(orderId, amount, PaymentStatus.FAILED, reason);
    }

    @PrePersist
    void onCreate() {
        this.createdAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public long getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
}
