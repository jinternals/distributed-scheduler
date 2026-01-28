package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.util.*;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class EventProcessorTest {

        @Mock
        private PartitionManager partitionManager;

        @Mock
        private EventRepository eventRepository;

        @Mock
        private PlatformTransactionManager transactionManager;

        @Mock
        private Executor eventTaskExecutor;

        private EventProcessor eventProcessor;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
                doAnswer(invocation -> {
                        ((Runnable) invocation.getArgument(0)).run();
                        return null;
                }).when(eventTaskExecutor).execute(any(Runnable.class));

                eventProcessor = new EventProcessor(partitionManager, eventRepository, transactionManager,
                                eventTaskExecutor);
        }

        @Test
        void testExecute_RoundRobinBehavior() {

                Set<Integer> activePartitions = new HashSet<>(Arrays.asList(1, 2, 3));
                when(partitionManager.getActivePartitions()).thenReturn(activePartitions);
                when(transactionManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));

                Event e1 = createEvent(1, 1);
                Event e2 = createEvent(2, 1);
                Event e3 = createEvent(2, 2);

                when(eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(1),
                                eq(EventStatus.PENDING)))
                                .thenReturn(Collections.singletonList(e1)) // 1st call
                                .thenReturn(Collections.emptyList()); // 2nd call

                when(eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(2),
                                eq(EventStatus.PENDING)))
                                .thenReturn(Collections.singletonList(e2)) // 1st call
                                .thenReturn(Collections.singletonList(e3)) // 2nd call
                                .thenReturn(Collections.emptyList()); // 3rd call

                when(eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(3),
                                eq(EventStatus.PENDING)))
                                .thenReturn(Collections.emptyList()); // 1st call

                // Mock saveAll to return the list passed to it (fluent-like)
                when(eventRepository.saveAll(anyList())).thenAnswer(invocation -> invocation.getArgument(0));

                eventProcessor.execute();

                verify(eventRepository, times(2)).findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(1),
                                eq(EventStatus.PENDING));
                verify(eventRepository, times(3)).findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(2),
                                eq(EventStatus.PENDING));
                verify(eventRepository, times(1)).findTop50ByPartitionIdAndStatusOrderByScheduledTime(eq(3),
                                eq(EventStatus.PENDING));

                // We expect 3 successful batches:
                // 1. Partition 1 (1 event) -> Save IN_PROGRESS -> Save PROCESSED
                // 2. Partition 2 (1 event) -> Save IN_PROGRESS -> Save PROCESSED
                // 3. Partition 2 (1 event) -> Save IN_PROGRESS -> Save PROCESSED
                // Total = 6 calls to saveAll
                verify(eventRepository, times(6)).saveAll(anyList());
                verify(eventRepository, never()).save(any(Event.class));

                assertEquals(EventStatus.PROCESSED, e1.getStatus());
                assertEquals(EventStatus.PROCESSED, e2.getStatus());
                assertEquals(EventStatus.PROCESSED, e3.getStatus());
        }

        private Event createEvent(int partitionId, long idSuffix) {
                Event event = new Event();
                event.setId(String.valueOf((long) partitionId * 1000 + idSuffix));
                event.setEventName("TestEvent");
                event.setPartitionId(partitionId);
                event.setStatus(EventStatus.PENDING);
                return event;
        }
}
