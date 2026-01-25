package com.jinternals.scheduler.api.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class TaskService {

    private final EventRepository eventRepository;

    @org.springframework.beans.factory.annotation.Value("${scheduler.partitions:6}")
    private int numPartitions;

    public TaskService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    @Transactional
    public Event scheduleTask(String name, LocalDateTime time, String payload) {
        Event event = new Event();
        event.setEventName(name);
        event.setScheduledTime(time);
        event.setPayload(payload);
        event.setStatus(EventStatus.PENDING);

        // Assign partition based on hash of name
        int partitionId = Math.abs(name.hashCode() % numPartitions);
        event.setPartitionId(partitionId);

        return eventRepository.save(event);
    }

    @Transactional
    public void removeTask(Long id) {
        eventRepository.deleteById(id);
    }
}
