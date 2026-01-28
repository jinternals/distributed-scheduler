package com.jinternals.scheduler.common.model;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {
    List<OutboxEvent> findTop500ByPartitionIdOrderByCreatedAt(Integer partitionId);
}
