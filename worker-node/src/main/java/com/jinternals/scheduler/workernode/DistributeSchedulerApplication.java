package com.jinternals.scheduler.workernode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EntityScan("com.jinternals.scheduler.common.model")
@EnableJpaRepositories("com.jinternals.scheduler.common.model")
@EnableScheduling
public class DistributeSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributeSchedulerApplication.class, args);
    }
}
