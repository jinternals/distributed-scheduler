package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
@Profile("!init & !controller")
public class EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
    private final PartitionManager partitionManager;
    private final EventRepository eventRepository;
    private final TransactionTemplate transactionTemplate;
    private final Executor eventTaskExecutor;

    public EventProcessor(PartitionManager partitionManager, EventRepository eventRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("eventTaskExecutor") Executor eventTaskExecutor) {
        this.partitionManager = partitionManager;
        this.eventRepository = eventRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.eventTaskExecutor = eventTaskExecutor;
    }

    @Scheduled(fixedDelay = 100)
    public void execute() {

        Set<Integer> partitions = partitionManager.getActivePartitions();
        if (partitions.isEmpty()) {
            logger.info("Polling: No active partitions assigned.");
            return;
        }

        List<Integer> activePartitions = new LinkedList<>(partitions);

        Collections.shuffle(activePartitions);

        // Round-Robin Loop
        while (!activePartitions.isEmpty()) {

            Iterator<Integer> iterator = activePartitions.iterator();

            while (iterator.hasNext()) {
                Integer partition = iterator.next();

                // Process ONE batch.
                boolean workFound = handlePartitionBatch(partition);

                // Optimization: If a partition is empty, remove it from the list
                // so we don't query it again during this cycle.
                if (!workFound) {
                    iterator.remove();
                }
            }
        }
    }

    private boolean handlePartitionBatch(Integer partition) {
        List<Event> eventsToProcess = fetchPendingEventsForPartition(partition);

        if (eventsToProcess == null || eventsToProcess.isEmpty()) {
            return false; // Signal that this partition is dry
        }

        List<CompletableFuture<Event>> futures = new ArrayList<>();

        for (Event event : eventsToProcess) {
            CompletableFuture<Event> future = CompletableFuture.supplyAsync(() -> handleEvent(event), eventTaskExecutor);
            futures.add(future);
        }

        List<Event> processedEvents = new ArrayList<>();
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            for (CompletableFuture<Event> future : futures) {
                processedEvents.add(future.join());
            }
        } catch (Exception e) {
            logger.error("Error waiting for batch completion", e);
        }

        // Step 3: Bulk Persist Final Results (Status -> PROCESSED/FAILED)
        if (!processedEvents.isEmpty()) {
            try {
                eventRepository.saveAll(processedEvents);
            } catch (Exception e) {
                logger.error("CRITICAL: Failed to save batch of {} processed events", processedEvents.size(), e);
            }
        }

        return true; // Signal that we did work
    }

    private List<Event> fetchPendingEventsForPartition(Integer partition) {
        List<Event> eventsToProcess = transactionTemplate.execute(status -> {
            logger.debug("Polling events for partition: {}", partition);
            List<Event> events = eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(
                    partition, EventStatus.PENDING);

            if (events.isEmpty()) {
                return Collections.emptyList();
            }

            events.forEach(event -> {
                event.setStatus(EventStatus.IN_PROGRESS);
                event.setLockedAt(LocalDateTime.now());
            });
            return eventRepository.saveAll(events);
        });
        return eventsToProcess;
    }

    private Event handleEvent(Event event) {
        logger.info("Handling event: {} [ID: {}] for Partition: {}", event.getEventName(), event.getId(),
                event.getPartitionId());
        try {
            extracted(event);
            event.setStatus(EventStatus.PROCESSED);
        } catch (Exception e) {
            logger.error("Error processing event", e);
            event.setStatus(EventStatus.FAILED);
            event.setExceptionStackTrace(getStackTrace(e));
        }
        return event;
    }

    private String getStackTrace(Exception e) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        e.printStackTrace(pw);
        String stackTrace = sw.toString();
        if (stackTrace.length() > 4000) {
            return stackTrace.substring(0, 4000);
        }
        return stackTrace;
    }

    private static void extracted(Event event) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
