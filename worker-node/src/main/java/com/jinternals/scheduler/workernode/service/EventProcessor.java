package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Service
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

        partitions.forEach(partition -> {
            Boolean processed;
            do {
                processed = transactionTemplate.execute(status -> {
                    logger.info("Polling events for partitions: {}", partition);
                    List<Event> pendingEvents = eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(
                            partition, EventStatus.PENDING);

                    if (pendingEvents.isEmpty()) {
                        return false;
                    }

                    for (Event event : pendingEvents) {
                        try {
                            handleEvent(event);
                        } catch (Exception e) {
                            logger.error("Error processing event transaction", e);
                        }
                    }
                    return true;
                });
            } while (Boolean.TRUE.equals(processed));
        });
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

    private static void extracted(Event event) throws InterruptedException {
        Thread.sleep(100);
    }
}
