package com.jinternals.scheduler.api.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PartitionUtils {
    public int partitionId(String id , int numPartitions) {
        return Math.abs(id.hashCode() % numPartitions);
    }
}
