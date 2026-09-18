package com.example.Notification;

public record PaymentCompletedEvent( Long orderId,
                                     Double amount,
                                     String status) {
}
