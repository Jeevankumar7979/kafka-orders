package com.jeevankumar.app.controller;

import com.jeevankumar.app.producer.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order-events")
public class OrderEventController {
    private final OrderProducer producer;


    public OrderEventController(OrderProducer producer) {
        this.producer = producer;
    }


    @PostMapping("/process")
    public void processOrder(@RequestParam String orderId){
        producer.processOrder(orderId);
    }

    @PostMapping("/processWithKey")
    public void processOrderWithKey(@RequestParam String orderId){
        producer.processOrderWithPartitionKey(orderId);
    }


}
