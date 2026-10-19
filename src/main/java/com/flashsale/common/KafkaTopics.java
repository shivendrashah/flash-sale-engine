package com.flashsale.common;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The names of our Kafka topics, and the topics themselves:
 *   flashsale.orders       events about orders ("ORDER_CONFIRMED"), 3 partitions
 *   flashsale.orders.DLT   the dead-letter topic: messages the notification worker gave up on
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopics {

    public static final String ORDERS = "flashsale.orders";

    /** "DLT" = dead-letter topic, the name Spring Kafka uses by convention. */
    public static final String ORDERS_DLT = ORDERS + ".DLT";

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic ordersDeadLetterTopic() {
        return TopicBuilder.name(ORDERS_DLT).partitions(1).replicas(1).build();
    }
}
