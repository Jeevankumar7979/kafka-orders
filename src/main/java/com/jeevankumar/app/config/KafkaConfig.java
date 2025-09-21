package com.jeevankumar.app.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${order.processing.topic-name}")
    private String TOPIC;

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }
}
