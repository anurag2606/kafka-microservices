package com.example.Order;

public record OrderCreatedEvent(Long orderId,
                                String product,
                                Double amount) {
}
