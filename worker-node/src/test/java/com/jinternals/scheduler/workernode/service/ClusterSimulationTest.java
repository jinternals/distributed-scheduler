package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.repositories.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import com.jinternals.scheduler.common.repositories.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

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

    // Shared "Outbox Database"
    private final Map<String, com.jinternals.scheduler.common.model.OutboxEvent> outboxDatabase = new ConcurrentHashMap<>();

    @Test
    void testSinglePartitionThroughput_withPublisher_10k_Events() throws InterruptedException {
        int totalEvents = 8000;
        int targetPartition = 1;

        // 1. Populate Database with 10k events in ONE partition
        System.out.println(
                "Populating database with " + totalEvents + " events for Partition " + targetPartition + "...");
        for (long i = 0; i < totalEvents; i++) {
            Event event = new Event();
            event.setId(String.valueOf(i));
            event.setEventName("Event-" + i);
            event.setPartitionId(targetPartition);
            event.setStatus(EventStatus.PENDING);
            database.put(event.getId(), event);
        }

        PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
        when(txManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));

        // Create *one* node assigned to this partition
        EventProcessor processor = createNode(0, Collections.singleton(targetPartition), txManager);

        // Create a Mock Publisher Runner
        ExecutorService publisherRunner = Executors.newSingleThreadExecutor();
        AtomicInteger publishedCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        // Run processor simulating 1s polling interval
        ExecutorService runner = Executors.newSingleThreadExecutor();
        runner.submit(() -> {
            while (true) {
                processor.execute();

                try {
                    Thread.sleep(100); // Shorter sleep to speed up test
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                long pendingCount = database.values().stream()
                        .filter(e -> e.getStatus() == EventStatus.PENDING)
                        .count();
                if (pendingCount == 0)
                    break;
            }
        });

        // Run Publisher simulating polling
        publisherRunner.submit(() -> {
            while (true) {
                // Fetch from Outbox DB
                List<com.jinternals.scheduler.common.model.OutboxEvent> batch = outboxDatabase.values().stream()
                        .filter(e -> e.getPartitionId() == targetPartition)
                        .limit(500)
                        .collect(Collectors.toList());

                if (!batch.isEmpty()) {
                    // Simulate Publish = Delete
                    batch.forEach(e -> outboxDatabase.remove(e.getId()));
                    publishedCount.addAndGet(batch.size());
                } else {
                    long pendingEvents = database.values().stream().filter(e -> e.getStatus() == EventStatus.PENDING)
                            .count();
                    if (pendingEvents == 0 && outboxDatabase.isEmpty()) {
                        break;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (Exception e) {
                }
            }
        });

        runner.shutdown();
        publisherRunner.shutdown();
        runner.awaitTermination(30, TimeUnit.SECONDS);
        publisherRunner.awaitTermination(30, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        System.out.println("End-to-End Throughput: " + totalEvents + " events in " + (endTime - startTime) + "ms");

        long processedCount = database.values().stream()
                .filter(e -> e.getStatus() == EventStatus.PROCESSED)
                .count();

        assertEquals(totalEvents, processedCount, "All events should be processed");
        assertEquals(0, outboxDatabase.size(), "Outbox should be empty");
        assertEquals(totalEvents, publishedCount.get(), "All events should be published");
    }

    private EventProcessor createNode(int nodeId, Set<Integer> partitions, PlatformTransactionManager txManager) {
        PartitionManager partitionManager = mock(PartitionManager.class);
        when(partitionManager.getActivePartitions()).thenReturn(partitions);

        EventRepository eventRepository = mock(EventRepository.class);

        when(eventRepository.findTop50ByPartitionIdAndStatusAndScheduledTimeLessThanEqualOrderByScheduledTime(
                anyInt(), eq(EventStatus.PENDING), any()))
                .thenAnswer(invocation -> {
                    int pId = invocation.getArgument(0);
                    return database.values().stream()
                            .filter(e -> e.getPartitionId() == pId && e.getStatus() == EventStatus.PENDING)
                            .sorted(Comparator.comparing(Event::getId))
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

        // Use Virtual Thread Executor (Same as Config)
        Executor executor = Executors.newVirtualThreadPerTaskExecutor();

        OutboxRepository outboxRepository = mock(OutboxRepository.class);
        ClockService clockService = mock(ClockService.class);

        // Mock Outbox Save
        when(outboxRepository.saveAll(anyList())).thenAnswer(invocation -> {
            List<com.jinternals.scheduler.common.model.OutboxEvent> batch = invocation.getArgument(0);
            for (com.jinternals.scheduler.common.model.OutboxEvent e : batch) {
                outboxDatabase.put(e.getId(), e);
            }
            return batch;
        });

        return new EventProcessor(partitionManager, eventRepository, outboxRepository, txManager, clockService, executor);
    }
}
