package com.jinternals.scheduler.api.controller;

import com.jinternals.scheduler.api.service.TaskService;
import com.jinternals.scheduler.common.model.Event;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public Event createTask(@RequestBody CreateTaskRequest request) {
        return taskService.scheduleTask(UUID.randomUUID().toString(), request.name(), request.scheduledTime(), request.payload());
    }

    @DeleteMapping("/{id}")
    public void deleteTask(@PathVariable String id) {
        taskService.removeTask(id);
    }
}

record CreateTaskRequest(String name, LocalDateTime scheduledTime, String payload) {}
