package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Service
public class EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
    private final PartitionManager partitionManager;
    private final EventRepository eventRepository;

    public EventProcessor(PartitionManager partitionManager, EventRepository eventRepository) {
        this.partitionManager = partitionManager;
        this.eventRepository = eventRepository;
    }

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processEvents() {
        Set<Integer> partitions = partitionManager.getActivePartitions();
        if (partitions.isEmpty()) {
            logger.info("Polling: No active partitions assigned.");
            return;
        }

        logger.info("Polling events for partitions: {}", partitions);
        // We find events that are PENDING, in the active partitions, and scheduled time
        // is <= now
        List<Event> pendingEvents = eventRepository.findByStatusAndPartitionIdInAndScheduledTimeBefore(
                "PENDING", partitions, LocalDateTime.now());

        for (Event event : pendingEvents) {
            handleEvent(event);
        }
    }

    private void handleEvent(Event event) {
        logger.info("Handling event: {} [ID: {}] for Partition: {}", event.getEventName(), event.getId(),
                event.getPartitionId());
        try {
            // Simulate processing
            Thread.sleep(100);
            event.setStatus("COMPLETED");
        } catch (Exception e) {
            logger.error("Error processing event", e);
            event.setStatus("FAILED");
        }
        eventRepository.save(event);
    }
}
