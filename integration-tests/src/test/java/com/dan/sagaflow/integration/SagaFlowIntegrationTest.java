package com.dan.sagaflow.integration;

import com.dan.sagaflow.common.event.Topics;
import com.dan.sagaflow.inventory.InventoryServiceApplication;
import com.dan.sagaflow.notification.NotificationServiceApplication;
import com.dan.sagaflow.order.OrderServiceApplication;
import com.dan.sagaflow.payment.PaymentServiceApplication;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * 진짜 Kafka(Testcontainers) 하나에 네 서비스를 같은 JVM의 별도 Spring 컨텍스트로 띄우는 공통 뼈대.
 *
 * <p>서비스마다 application.yml 이름이 같아 클래스패스에서 하나만 읽히므로, 서비스별로 달라야 하는 값
 * (포트, 이름, DB, 컨슈머 그룹)은 커맨드라인 인자로 덮어쓴다. DB는 H2 인메모리를 쓴다.
 * Docker가 없으면 테스트는 실패하지 않고 건너뛴다.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class SagaFlowIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.0");

    static ConfigurableApplicationContext orderContext;
    static ConfigurableApplicationContext inventoryContext;
    static ConfigurableApplicationContext paymentContext;
    static ConfigurableApplicationContext notificationContext;

    /** outbox를 쓰는 서비스(order, inventory, payment)는 직렬화해 둔 JSON 문자열을 그대로 보내고, notification은 이벤트를 발행하지 않으므로 형식은 의미가 없지만 기존 JSON 설정을 그대로 둔다. */
    static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    static final String JSON_SERIALIZER = "org.springframework.kafka.support.serializer.JacksonJsonSerializer";

    static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void startServices() {
        createTopics();
        orderContext = start(OrderServiceApplication.class, "order-service", "order", STRING_SERIALIZER);
        inventoryContext = start(InventoryServiceApplication.class, "inventory-service", "inventory", STRING_SERIALIZER);
        paymentContext = start(PaymentServiceApplication.class, "payment-service", "payment", STRING_SERIALIZER);
        notificationContext = start(NotificationServiceApplication.class, "notification-service", "notification", JSON_SERIALIZER);
    }

    @AfterAll
    static void stopServices() {
        for (ConfigurableApplicationContext context :
                new ConfigurableApplicationContext[] {notificationContext, paymentContext, inventoryContext, orderContext}) {
            if (context != null) {
                context.close();
            }
        }
    }

    /** 컨슈머가 뜨기 전에 토픽이 있어야 파티션 할당이 빨리 끝난다. */
    private static void createTopics() {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDER_EVENTS, 1, (short) 1),
                    new NewTopic(Topics.INVENTORY_EVENTS, 1, (short) 1),
                    new NewTopic(Topics.PAYMENT_EVENTS, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("토픽 생성 실패", e);
        }
    }

    private static ConfigurableApplicationContext start(
            Class<?> application, String name, String dbName, String producerValueSerializer) {
        return new SpringApplicationBuilder(application).run(
                "--server.port=0",
                "--spring.application.name=" + name,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=" + name,
                "--spring.kafka.producer.value-serializer=" + producerValueSerializer,
                "--spring.kafka.producer.properties.spring.json.add.type.headers=false",
                "--spring.datasource.url=jdbc:h2:mem:" + dbName + "_it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "--spring.datasource.username=sa",
                "--spring.datasource.password=",
                "--spring.jpa.hibernate.ddl-auto=create-drop");
    }

    /** order-service의 POST /orders를 호출하고 응답 본문(JSON)에서 주문 ID를 꺼내 돌려준다. */
    static long placeOrder(long customerId, long productId, int quantity, long unitPrice)
            throws IOException, InterruptedException {
        String port = orderContext.getEnvironment().getProperty("local.server.port");
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"customerId\":%d,\"productId\":%d,\"quantity\":%d,\"unitPrice\":%d}"
                                .formatted(customerId, productId, quantity, unitPrice)))
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new IllegalStateException("주문 생성 실패 status=" + response.statusCode() + " body=" + response.body());
        }
        Matcher m = Pattern.compile("\"orderId\"\\s*:\\s*(\\d+)").matcher(response.body());
        if (!m.find()) {
            throw new IllegalStateException("응답에 orderId가 없음: " + response.body());
        }
        return Long.parseLong(m.group(1));
    }
}
