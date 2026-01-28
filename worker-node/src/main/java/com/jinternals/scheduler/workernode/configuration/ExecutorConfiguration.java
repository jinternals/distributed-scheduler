package com.jinternals.scheduler.workernode.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executor;

@Configuration
public class ExecutorConfiguration {

    @Bean(name = "eventTaskExecutor")
    public Executor eventTaskExecutor() {
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }
}
