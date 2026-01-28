package com.jinternals.scheduler.common.model;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "events")
public class Event {
    @Id
    private String id;

    private String eventName;

    private LocalDateTime scheduledTime;

    private String payload;

    @Column(name = "exception_stack_trace")
    private String exceptionStackTrace;

    @Column(name = "locked_at")
    private LocalDateTime lockedAt;

    private int partitionId;

    @Enumerated(EnumType.STRING)
    private EventStatus status;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
