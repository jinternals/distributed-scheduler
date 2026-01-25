package com.jinternals.scheduler.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("com.jinternals.scheduler.common.model")
@EnableJpaRepositories("com.jinternals.scheduler.common.model")
public class SchedulerApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApiApplication.class, args);
    }

}
