package com.jinternals.scheduler.workernode.service;

import org.springframework.stereotype.Service;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PartitionManager {
    private final Set<Integer> activePartitions = ConcurrentHashMap.newKeySet();

    public void addPartition(int partitionId) {
        activePartitions.add(partitionId);
        System.out.println("PartitionManager: Added partition " + partitionId);
    }

    public void removePartition(int partitionId) {
        activePartitions.remove(partitionId);
        System.out.println("PartitionManager: Removed partition " + partitionId);
    }

    public Set<Integer> getActivePartitions() {
        return activePartitions;
    }
}
