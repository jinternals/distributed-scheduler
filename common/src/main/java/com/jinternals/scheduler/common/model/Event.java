package com.jinternals.scheduler.common.model;

import jakarta.persistence.*;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.ReadOnlyProperty;

import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "events")
public class Event {
    @Id
    private String id;

    private String eventName;

    private String namespace;

    private LocalDateTime scheduledTime;

    private String payload;

    @Column(name = "exception_stack_trace", length = 4096)
    private String exceptionStackTrace;

    @Column(name = "locked_at")
    private LocalDateTime lockedAt;

    private int partitionId;

    @Enumerated(EnumType.STRING)
    private EventStatus status;

    @CreatedDate
    @ReadOnlyProperty
    private LocalDateTime createdAt;

    @LastModifiedDate
    private LocalDateTime updatedAt;

}
