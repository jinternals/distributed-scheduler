package com.jinternals.scheduler.common.model;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "events")
public class Event {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String eventName;

    private LocalDateTime scheduledTime;

    private String payload;

    // This is the key for distribution
    private int partitionId;

    private String status; // PENDING, PROCESSED, FAILED
}
