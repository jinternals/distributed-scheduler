package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;

@Service
@Profile("!init & !controller")
public class EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
    private final PartitionManager partitionManager;
    private final EventRepository eventRepository;
    private final TransactionTemplate transactionTemplate;

    public EventProcessor(PartitionManager partitionManager, EventRepository eventRepository,
            PlatformTransactionManager transactionManager) {
        this.partitionManager = partitionManager;
        this.eventRepository = eventRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelay = 5000)
    public void execute() {

        Set<Integer> partitions = partitionManager.getActivePartitions();
        if (partitions.isEmpty()) {
            logger.info("Polling: No active partitions assigned.");
            return;
        }

        List<Integer>activePartitions = new LinkedList<>(partitions);

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
        return Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            // Log at debug to reduce noise during high throughput
            logger.info("Polling events for partition: {}", partition);

            List<Event> pendingEvents = eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(
                    partition, EventStatus.PENDING);

            if (pendingEvents.isEmpty()) {
                return false; // Signal that this partition is dry
            }

            for (Event event : pendingEvents) {
                try {
                    handleEvent(event);
                } catch (Exception e) {
                    logger.error("Error processing event transaction", e);
                }
            }
            return true; // Signal that we did work
        }));
    }

    private void handleEvent(Event event) {
        logger.info("Handling event: {} [ID: {}] for Partition: {}", event.getEventName(), event.getId(),
                event.getPartitionId());
        try {
            extracted(event);
            event.setStatus(EventStatus.PROCESSED);
        } catch (Exception e) {
            logger.error("Error processing event", e);
            event.setStatus(EventStatus.FAILED);
        }
        eventRepository.save(event);
    }

    private static void extracted(Event event) {
        System.out.println("event" + event);
    }
}
