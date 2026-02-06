package com.jinternals.scheduler.api.utils;


public class PartitionUtils {

    private PartitionUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static int partitionId(String id, int numPartitions) {
        return Math.abs(id.hashCode() % numPartitions);
    }

}
