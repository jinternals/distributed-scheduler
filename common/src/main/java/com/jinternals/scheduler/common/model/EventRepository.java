package com.jinternals.scheduler.common.model;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface EventRepository extends JpaRepository<Event, Long> {
    List<Event> findByPartitionIdAndStatus(int partitionId, String status);

    List<Event> findByStatusAndPartitionIdInAndScheduledTimeBefore(String status, java.util.Collection<Integer> partitionIds, java.time.LocalDateTime scheduledTime);
}
