package com.jinternals.scheduler.api.controller;

import com.jinternals.scheduler.api.service.EventSchedulerService;
import com.jinternals.scheduler.common.model.Event;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/event")
public class EventSchedulerController {

    private final EventSchedulerService eventSchedulerService;

    public EventSchedulerController(EventSchedulerService eventSchedulerService) {
        this.eventSchedulerService = eventSchedulerService;
    }

    @PostMapping
    public Event scheduleEvent(@RequestBody CreateTaskRequest request) {
        String eventId = request.id() != null ? request.id() : UUID.randomUUID().toString();
        return eventSchedulerService.scheduleEvent(eventId, request.name(), request.scheduledTime(), request.payload(),
                request.namespace());
    }

    @GetMapping("/{id}")
    public Event getEvent(@PathVariable String id) {
        return eventSchedulerService.getEvent(id);
    }

    @DeleteMapping("/{id}")
    public void removedScheduledEvent(@PathVariable String id) {
        eventSchedulerService.removeEvent(id);
    }
}

record CreateTaskRequest(String id, String name, LocalDateTime scheduledTime, String payload, String namespace) {
}
