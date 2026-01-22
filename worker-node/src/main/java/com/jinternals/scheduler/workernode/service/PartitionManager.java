package com.jinternals.scheduler.workernode.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class PartitionManager {
    private final Set<Integer> activePartitions = ConcurrentHashMap.newKeySet();

    public void addPartition(int partitionId) {
        activePartitions.add(partitionId);
        log.info("PartitionManager: Added partition {}" , partitionId);
    }

    public void removePartition(int partitionId) {
        activePartitions.remove(partitionId);
        log.info("PartitionManager: Removed partition {}" , partitionId);
    }

    public Set<Integer> getActivePartitions() {
        return activePartitions;
    }
}
