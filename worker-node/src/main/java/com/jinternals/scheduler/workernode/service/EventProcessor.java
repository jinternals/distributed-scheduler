package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.*;
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
import java.util.concurrent.Executor;

import static com.jinternals.scheduler.common.model.EventStatus.PENDING;
import static com.jinternals.scheduler.common.model.EventStatus.PROCESSED;

@Service
@Profile("!init & !controller")
public class EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
    private final PartitionManager partitionManager;
    private final EventRepository eventRepository;
    private final OutboxRepository outboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final Executor eventTaskExecutor;

    private static final int POLL_INTERVAL_MS = 1000; // Increased poll frequency since we lost wheel precision

    public EventProcessor(PartitionManager partitionManager, EventRepository eventRepository,
            OutboxRepository outboxRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("eventTaskExecutor") Executor eventTaskExecutor) {
        this.partitionManager = partitionManager;
        this.eventRepository = eventRepository;
        this.outboxRepository = outboxRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.eventTaskExecutor = eventTaskExecutor;
    }

    @Scheduled(fixedDelay = POLL_INTERVAL_MS)
    public void execute() {

        Set<Integer> partitions = partitionManager.getActivePartitions();
        if (partitions.isEmpty()) {
            logger.info("Polling: No active partitions assigned.");
            return;
        }

        List<Integer> activePartitions = new LinkedList<>(partitions);
        Collections.shuffle(activePartitions);

        LocalDateTime now = LocalDateTime.now();

        for (Integer partition : activePartitions) {
            handlePartitionBatch(partition, now);
        }
    }

    private void handlePartitionBatch(Integer partition, LocalDateTime now) {
        // Offload partition draining to a virtual thread
        eventTaskExecutor.execute(() -> {
            int batchCount = 0;
            // Drain up to 20 batches (1000 events) per poll cycle to prevent starvation of
            // other partitions
            // or holding resources too long.
            while (batchCount < 20) {
                List<Event> eventsToProcess = fetchPendingEventsForPartition(partition, now);

                if (eventsToProcess.isEmpty()) {
                    break;
                }

                processBatch(eventsToProcess);
                batchCount++;
            }
        });
    }

    private List<Event> fetchPendingEventsForPartition(Integer partition, LocalDateTime now) {
        return transactionTemplate.execute(status -> {
            List<Event> events = eventRepository
                    .findTop50ByPartitionIdAndStatusAndScheduledTimeLessThanEqualOrderByScheduledTime(
                            partition, PENDING, now);

            if (events.isEmpty()) {
                return Collections.emptyList();
            }

            events.forEach(event -> {
                event.setStatus(EventStatus.IN_PROGRESS);
                event.setLockedAt(LocalDateTime.now());
            });
            return eventRepository.saveAll(events);
        });
    }

    private void processBatch(List<Event> events) {
        transactionTemplate.execute(status -> {
            try {
                List<OutboxEvent> outboxEvents = events.stream()
                        .map(event -> OutboxEvent.builder()
                                .id(UUID.randomUUID().toString())
                                .aggregateId(event.getId())
                                .aggregateType("EVENT")
                                .payload(event.getPayload())
                                .partitionId(event.getPartitionId())
                                .createdAt(LocalDateTime.now())
                                .build())
                        .toList();

                outboxRepository.saveAll(outboxEvents);

                events.forEach(event -> event.setStatus(PROCESSED));
                eventRepository.saveAll(events);

                logger.info("Processed batch of {} events for partition {}", events.size(),
                        events.stream().findFirst().map(Event::getPartitionId).orElse(-1));

            } catch (Exception e) {
                logger.error("Error processing batch", e);
                status.setRollbackOnly();
            }
            return null;
        });
    }

}
