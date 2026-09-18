package com.example.Payment;

public record PaymentCompletedEvent(
        Long orderId,
        Double amount,
        String status
) {
}
