package com.jinternals.scheduler.common.model;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.hibernate.LockOptions;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface EventRepository extends JpaRepository<Event, Long> {

    @QueryHints({@QueryHint(name = QueryHintsUtils.TIMEOUT_HINT_NAME, value = QueryHintsUtils.UPGRADE_SKIPLOCKED)})
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Event> findTop50ByPartitionIdAndStatusOrderByScheduledTime(int partitionId, EventStatus status);

    @Modifying
    @Query("UPDATE Event e SET e.status = :newStatus, e.lockedAt = NULL WHERE e.status = :oldStatus AND e.lockedAt < :cutoff AND e.partitionId IN :partitions")
    int resetStuckEvents(@Param("oldStatus") EventStatus oldStatus,
            @Param("newStatus") EventStatus newStatus,
            @Param("cutoff") java.time.LocalDateTime cutoff,
            @Param("partitions") List<Integer> partitions);
}
