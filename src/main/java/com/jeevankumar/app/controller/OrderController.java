package com.jeevankumar.app.controller;

import com.jeevankumar.app.event.OrderEvent;
import com.jeevankumar.app.producer.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/demo/no-key")
    public String demoNoKey(@RequestParam String orderId, @RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            producer.sendWithoutKey(new OrderEvent(orderId, i, "EV_" + i, Instant.now()));
        }
        return "sent no-key";
    }

    // Run this demo endpoints to see difference in ordering with keys
    // single partition
    @PostMapping("/demo/with-key")
    public String demoWithKey(@RequestParam String orderId, @RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            producer.sendWithKey(new OrderEvent(orderId, i, "EV_" + i, Instant.now()));
        }
        return "sent with-key";
    }

}
