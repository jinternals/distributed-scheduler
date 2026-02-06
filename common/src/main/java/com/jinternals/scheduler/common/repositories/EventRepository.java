package com.jinternals.scheduler.common.repositories;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventStatus;
import com.jinternals.scheduler.common.QueryHintsUtils;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface EventRepository extends JpaRepository<Event, String> {

    @QueryHints({@QueryHint(name = QueryHintsUtils.TIMEOUT_HINT_NAME, value = QueryHintsUtils.UPGRADE_SKIP_LOCKED)})
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Event> findTop50ByPartitionIdAndStatusOrderByScheduledTime(int partitionId, EventStatus status);

    @QueryHints({@QueryHint(name = QueryHintsUtils.TIMEOUT_HINT_NAME, value = QueryHintsUtils.UPGRADE_SKIP_LOCKED)})
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Event> findTop50ByPartitionIdAndStatusAndScheduledTimeLessThanEqualOrderByScheduledTime(int partitionId,
                                                                                                 EventStatus status, LocalDateTime scheduledTime);

    @Modifying
    @Query("UPDATE Event e SET e.status = :newStatus, e.lockedAt = NULL WHERE e.status = :oldStatus AND e.lockedAt < :cutoff AND e.partitionId IN :partitions")
    int resetStuckEvents(@Param("oldStatus") EventStatus oldStatus,
                         @Param("newStatus") EventStatus newStatus,
                         @Param("cutoff") LocalDateTime cutoff,
                         @Param("partitions") List<Integer> partitions);
}
