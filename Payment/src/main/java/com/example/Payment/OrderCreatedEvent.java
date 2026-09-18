package com.example.Payment;

public record OrderCreatedEvent(Long orderId,
                                String product,
                                Double amount) {
}
