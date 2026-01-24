package com.jinternals.scheduler.common.model;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;

public interface EventRepository extends JpaRepository<Event, Long> {

    @QueryHints({ @QueryHint(name = QueryHintsUtils.TIMEOUT_HINT_NAME, value = QueryHintsUtils.UPGRADE_SKIPLOCKED) })
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Event> findTop50ByPartitionIdAndStatusOrderByScheduledTime(int partitionId, EventStatus status);

}
