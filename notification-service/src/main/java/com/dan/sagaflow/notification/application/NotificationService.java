package com.dan.sagaflow.notification.application;

import com.dan.sagaflow.notification.domain.Notification;
import com.dan.sagaflow.notification.domain.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public Notification record(Long orderId, String eventType, String message) {
        Notification saved = notificationRepository.save(new Notification(orderId, eventType, message));
        log.info("알림 기록 orderId={} type={} message={}", orderId, eventType, message);
        return saved;
    }
}
