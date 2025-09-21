package com.jeevankumar.app.consumer;

import com.jeevankumar.app.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    //@KafkaListener(topics = "${order.processing.topic-name}", groupId = "order-group")
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        System.out.printf("CONSUMER: partition=%d offset=%d orderId=%s seq=%d event=%s%n",
                record.partition(), record.offset(),
                event.orderId(), event.seq(), event.eventType());
    }
}
