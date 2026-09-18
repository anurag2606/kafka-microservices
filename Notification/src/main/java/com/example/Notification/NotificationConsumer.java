package com.example.Notification;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    @KafkaListener(
            topics = "payment-completed",
            groupId = "notification-group"
    )
    public void consume(PaymentCompletedEvent event) {

        System.out.println("===== Notification Service =====");

        System.out.println("Order ID : " + event.orderId());
        System.out.println("Amount   : " + event.amount());
        System.out.println("Status   : " + event.status());

        System.out.println("Notification sent successfully!");
    }
}
