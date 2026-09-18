package com.example.Order;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderService {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderService(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public String createOrder(OrderRequest request) {

        Long orderId = System.currentTimeMillis();

        OrderCreatedEvent event =
                new OrderCreatedEvent(
                        orderId,
                        request.product(),
                        request.amount()
                );

        kafkaTemplate.send(
                "order-created",
                orderId.toString(),
                event
        );

        return "Order created: " + orderId;
    }
}
