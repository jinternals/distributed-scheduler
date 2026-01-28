package com.jinternals.scheduler.api.service;

import com.jinternals.scheduler.api.utils.PartitionUtils;
import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

import static com.jinternals.scheduler.api.utils.PartitionUtils.partitionId;

@Service
public class TaskService {

    private final EventRepository eventRepository;

    @org.springframework.beans.factory.annotation.Value("${scheduler.partitions:6}")
    private int numPartitions;

    public TaskService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    @Transactional
    public Event scheduleTask(String id, String name, LocalDateTime time, String payload) {
        Event event = new Event();
        event.setId(id);
        event.setEventName(name);
        event.setScheduledTime(time);
        event.setPayload(payload);
        event.setStatus(EventStatus.PENDING);
        event.setPartitionId(partitionId(id, numPartitions));
        return eventRepository.save(event);
    }

    @Transactional
    public void removeTask(String id) {
        eventRepository.deleteById(id);
    }
}
