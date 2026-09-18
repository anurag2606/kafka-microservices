package com.example.Payment;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {

    private final KafkaTemplate<String, PaymentCompletedEvent>
            kafkaTemplate;

    public PaymentConsumer(
            KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "order-created",
            groupId = "payment-group"
    )
    public void consume(OrderCreatedEvent event) {

        System.out.println("================================");
        System.out.println("Payment Service");
        System.out.println("Order ID: " + event.orderId());
        System.out.println("Product: " + event.product());
        System.out.println("Amount: " + event.amount());

        // Payment processing
        String status = "SUCCESS";

        PaymentCompletedEvent paymentEvent =
                new PaymentCompletedEvent(
                        event.orderId(),
                        event.amount(),
                        status
                );

        kafkaTemplate.send(
                "payment-completed",
                event.orderId().toString(),
                paymentEvent
        );

        System.out.println("Payment completed event sent");
        System.out.println("================================");
    }

//    private boolean processPayment(OrderCreatedEvent event) {
//
//        System.out.println(
//                "Processing payment: "
//                        + event.amount()
//        );
//
//        return true;
//    }
}