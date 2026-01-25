package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;

@Service
@Slf4j
@Profile("!init & !controller")
public class StuckEventReaper {

    private final EventRepository eventRepository;
    private final PartitionManager partitionManager;

    public StuckEventReaper(EventRepository eventRepository, PartitionManager partitionManager) {
        this.eventRepository = eventRepository;
        this.partitionManager = partitionManager;
    }

    @Scheduled(fixedDelay = 60000)
    @Transactional
    public void recoverStuckEvents() {
        var activePartitions = partitionManager.getActivePartitions();
        if (activePartitions.isEmpty()) {
            return;
        }

        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(10);
        int updatedCount = eventRepository.resetStuckEvents(
                EventStatus.IN_PROGRESS,
                EventStatus.PENDING,
                cutoff,
                new ArrayList<>(activePartitions));

        if (updatedCount > 0) {
            log.warn("REAPER: Recovered {} stuck events (locked before {}) for partitions {}",
                    updatedCount, cutoff, activePartitions);
        }
    }
}
