package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ClusterSimulationTest {

    // Shared "Database"
    private final Map<String, Event> database = new ConcurrentHashMap<>();

    @Test
    void simulateClusterProcessing_10k_Events() throws InterruptedException {
        int totalEvents = 10000;
        int partitions = 6;
        int nodes = 3;

        // 1. Populate Database
        System.out.println("Populating database with " + totalEvents + " events...");
        for (long i = 0; i < totalEvents; i++) {
            Event event = new Event();
            event.setId(String.valueOf(i));
            event.setEventName("Event-" + i);
            event.setPartitionId((int) (i % partitions));
            event.setStatus(EventStatus.PENDING);
            database.put(String.valueOf(i), event);
        }

        List<EventProcessor> processors = new ArrayList<>();
        ExecutorService nodeExecutor = Executors.newFixedThreadPool(nodes);

        PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
        when(txManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));

        for (int i = 0; i < nodes; i++) {
            Set<Integer> assignedPartitions = new HashSet<>();
            assignedPartitions.add(i * 2);
            assignedPartitions.add(i * 2 + 1);

            processors.add(createNode(i, assignedPartitions, txManager));
        }

        long startTime = System.currentTimeMillis();
        AtomicInteger activeNodes = new AtomicInteger(nodes);

        for (int i = 0; i < nodes; i++) {
            final int nodeId = i;
            final EventProcessor processor = processors.get(i);

            nodeExecutor.submit(() -> {
                System.out.println("Node " + nodeId + " started.");
                while (true) {
                    try {
                        processor.execute();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    long pendingCount = database.values().stream()
                            .filter(e -> e.getStatus() == EventStatus.PENDING)
                            .count();

                    if (pendingCount == 0) {
                        break;
                    }
                }
                System.out.println("Node " + nodeId + " finished.");
                activeNodes.decrementAndGet();
            });
        }

        nodeExecutor.shutdown();
        nodeExecutor.awaitTermination(60, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        System.out.println("Processing finished in " + (endTime - startTime) + "ms");

        long processedCount = database.values().stream()
                .filter(e -> e.getStatus() == EventStatus.PROCESSED)
                .count();

        assertEquals(totalEvents, processedCount, "All events should be processed");
    }

    private EventProcessor createNode(int nodeId, Set<Integer> partitions, PlatformTransactionManager txManager) {
        PartitionManager partitionManager = mock(PartitionManager.class);
        when(partitionManager.getActivePartitions()).thenReturn(partitions);

        EventRepository eventRepository = mock(EventRepository.class);

        when(eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(anyInt(), eq(EventStatus.PENDING)))
                .thenAnswer(invocation -> {
                    int pId = invocation.getArgument(0);
                    return database.values().stream()
                            .filter(e -> e.getPartitionId() == pId && e.getStatus() == EventStatus.PENDING)
                            .sorted(Comparator.comparing(Event::getId)) // Simple sort
                            .limit(50)
                            .collect(Collectors.toList());
                });

        when(eventRepository.saveAll(anyList())).thenAnswer(invocation -> {
            List<Event> batch = invocation.getArgument(0);
            for (Event e : batch) {
                database.put(e.getId(), e);
            }
            return batch;
        });

        when(eventRepository.save(any(Event.class))).thenAnswer(invocation -> {
            Event e = invocation.getArgument(0);
            database.put(e.getId(), e);
            return e;
        });

        // Real Executor for the node's parallelism
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setThreadNamePrefix("Node-" + nodeId + "-Worker-");
        executor.initialize();

        return new EventProcessor(partitionManager, eventRepository, txManager, executor);
    }
}
