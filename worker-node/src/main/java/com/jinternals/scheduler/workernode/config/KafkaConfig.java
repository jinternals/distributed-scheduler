package com.jinternals.scheduler.workernode.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("!init & !controller")
public class KafkaConfig {

    public static final String SCHEDULER_EVENTS_TOPIC = "scheduler-events";

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(SCHEDULER_EVENTS_TOPIC)
                .partitions(12)
                .replicas(1)
                .build();
    }
}
