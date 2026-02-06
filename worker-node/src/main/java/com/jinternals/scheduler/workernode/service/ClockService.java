package com.jinternals.scheduler.workernode.service;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ClockService {

    public LocalDateTime getCurrentDateTime(){
       return LocalDateTime.now();
    }
}
