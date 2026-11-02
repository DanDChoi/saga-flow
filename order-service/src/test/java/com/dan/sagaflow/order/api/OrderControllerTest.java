package com.dan.sagaflow.order.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.dan.sagaflow.order.domain.Order;
import com.dan.sagaflow.order.domain.OrderRepository;
import com.dan.sagaflow.order.domain.OrderStatus;
import com.dan.sagaflow.order.application.OrderEventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderControllerTest {

    @Autowired
    MockMvc mockMvc;

    // Kafka 브로커 없이 테스트하기 위해 발행기는 목으로 대체한다.
    @MockitoBean
    OrderEventPublisher orderEventPublisher;

    @Autowired
    OrderRepository orderRepository;

    @Test
    void 주문을_생성하면_201과_함께_CREATED_상태로_저장된다() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId": 1, "productId": 100, "quantity": 3, "unitPrice": 5000}
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.orderId").isNumber())
                .andExpect(jsonPath("$.totalAmount").value(15000))
                .andExpect(jsonPath("$.status").value("CREATED"));

        assertThat(orderRepository.findAll())
                .extracting(Order::getStatus, Order::getTotalAmount)
                .contains(tuple(OrderStatus.CREATED, 15000L));
    }

    @Test
    void 수량이_1보다_작으면_400이다() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId": 1, "productId": 100, "quantity": 0, "unitPrice": 5000}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 필수값이_없으면_400이다() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 100, "quantity": 1, "unitPrice": 5000}
                                """))
                .andExpect(status().isBadRequest());
    }
}
