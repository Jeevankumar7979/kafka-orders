package com.jeevankumar.app.producer;

import com.jeevankumar.app.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class OrderProducer {

    @Value("${order.processing.topic-name}")
    private String TOPIC;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendWithoutKey(OrderEvent event) {
        kafkaTemplate.send(TOPIC, event);
        System.out.println("Event produced: " + event);
    }

    public void sendWithKey(OrderEvent event) {
        kafkaTemplate.send(TOPIC, event.orderId(), event);
        System.out.println("Event produced: " + event);
    }

    public void processOrder(String orderId) {
        System.out.println("Processing order without key: " + orderId);
        kafkaTemplate.send(TOPIC,
                new OrderEvent(orderId, 1, "OrderPlaced", Instant.now()));
        kafkaTemplate.send(TOPIC,
                new OrderEvent(orderId, 2, "PaymentProcessed", Instant.now()));
        kafkaTemplate.send(TOPIC,
                new OrderEvent(orderId, 3, "OrderShipped", Instant.now()));
        kafkaTemplate.send(TOPIC,
                new OrderEvent(orderId, 4, "OrderDelivered", Instant.now()));
        System.out.println("Completed processing order without key: " + orderId);
    }

    public void processOrderWithPartitionKey(String orderId) {
        System.out.println("Processing order without key: " + orderId);
        kafkaTemplate.send(TOPIC,orderId,
                new OrderEvent(orderId, 1, "OrderPlaced", Instant.now()));
        kafkaTemplate.send(TOPIC,orderId,
                new OrderEvent(orderId, 2, "PaymentProcessed", Instant.now()));
        kafkaTemplate.send(TOPIC,orderId,
                new OrderEvent(orderId, 3, "OrderShipped", Instant.now()));
        kafkaTemplate.send(TOPIC,orderId,
                new OrderEvent(orderId, 4, "OrderDelivered", Instant.now()));
        System.out.println("Completed processing order with partition key: " + orderId);
    }

    public void send(){
        kafkaTemplate.send(TOPIC,
                new OrderEvent("order-123", 1, "OrderPlaced", Instant.now()));
        System.out.println("Event produced");
    }

    public void sendError(){
        kafkaTemplate.send(TOPIC,
                new OrderEvent("order-123", 1, "ErrorEvent", Instant.now()));
        System.out.println("Error Event produced");
    }

    public void feature5commit1(){
        System.out.println("feature5commit1");
    }

    public void hello(){
        System.out.println("Hello from OrderProducer");
    }

    public void feature6(){
        System.out.println("feature6");
        System.out.println("feature6-2");
    }

    public void feature7() {
        System.out.println("Feature 7 implementation");
        System.out.println("Feature 7 additional log");
    }

    public void feature9() {
        System.out.println("Feature 9 implementation");
        System.out.println("Feature 9 additional log");
    }

    public void feature8() {
        System.out.println("Feature 8 implementation");
        System.out.println("Feature 8 additional log");
    }
}
