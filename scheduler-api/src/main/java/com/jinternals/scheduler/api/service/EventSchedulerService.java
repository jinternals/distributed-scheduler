package com.jinternals.scheduler.api.service;

import com.jinternals.scheduler.api.exceptions.EventNotFoundException;
import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.repositories.EventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

import static com.jinternals.scheduler.api.utils.PartitionUtils.partitionId;
import static com.jinternals.scheduler.common.model.EventStatus.PENDING;

import org.springframework.beans.factory.annotation.Value;

@Service
public class EventSchedulerService {

    private final EventRepository eventRepository;

    @Value("${scheduler.partitions:6}")
    private int numPartitions;

    public EventSchedulerService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    @Transactional
    public Event scheduleEvent(String id, String name, LocalDateTime time, String payload, String namespace) {
        Event event = new Event();
        event.setId(id);
        event.setEventName(name);
        event.setNamespace(namespace);
        event.setScheduledTime(time);
        event.setPayload(payload);
        event.setStatus(PENDING);
        event.setPartitionId(partitionId(id, numPartitions));
        return eventRepository.save(event);
    }

    @Transactional
    public void removeEvent(String id) {
        eventRepository.deleteById(id);
    }

    public Event getEvent(String id) {
        return eventRepository
                .findById(id)
                .orElseThrow(() -> new EventNotFoundException(id + " not found"));
    }
}
