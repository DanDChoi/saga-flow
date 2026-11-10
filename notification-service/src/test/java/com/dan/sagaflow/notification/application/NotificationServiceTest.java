package com.dan.sagaflow.notification.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.dan.sagaflow.notification.domain.Notification;
import com.dan.sagaflow.notification.domain.NotificationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class NotificationServiceTest {

    @Autowired
    NotificationService notificationService;

    @Autowired
    NotificationRepository notificationRepository;

    @Test
    void 알림을_기록하면_주문ID로_조회된다() {
        notificationService.record(42L, "PAYMENT_COMPLETED", "결제 완료");

        Notification found = notificationRepository.findByOrderIdOrderByIdAsc(42L).get(0);
        assertThat(found.getEventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(found.getMessage()).isEqualTo("결제 완료");
        assertThat(found.getCreatedAt()).isNotNull();
    }
}
