package com.jinternals.scheduler.workernode;

import com.jinternals.scheduler.common.model.Event;
import com.jinternals.scheduler.common.model.EventRepository;
import com.jinternals.scheduler.common.model.EventStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ComponentScan(basePackages = "com.jinternals.scheduler.common.model")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class EventRepositoryTest {

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private org.springframework.transaction.PlatformTransactionManager transactionManager;

    @AfterEach
    public void cleanup() {
        eventRepository.deleteAll();
    }

    @Test
    public void testSkipLocked() throws Exception {
        // Given: 2 events in partition 1
        Event event1 = createEvent("Event 1", 1);
        Event event2 = createEvent("Event 2", 1);

        event1 = eventRepository.save(event1);
        event2 = eventRepository.save(event2);

        Long lockedId = event1.getId();

        // Coordination
        CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
        CountDownLatch testCompleteLatch = new CountDownLatch(1);

        // Thread 1: Acquires lock on Event 1 using JDBC
        Thread t1 = new Thread(() -> {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    System.out.println("Thread 1: Attempting to lock ID " + lockedId);
                    stmt.execute("SELECT * FROM events WHERE id = " + lockedId + " FOR UPDATE");
                    System.out.println("Thread 1: Locked ID " + lockedId);

                    lockAcquiredLatch.countDown();

                    // Hold lock until test main thread signals
                    testCompleteLatch.await(5, TimeUnit.SECONDS);
                    conn.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.start();

        try {
            // Wait for T1 to acquire lock
            boolean locked = lockAcquiredLatch.await(5, TimeUnit.SECONDS);
            assertThat(locked).as("Background thread failed to acquire lock").isTrue();

            // Thread 2 (Main): Fetch events with SKIP LOCKED
            // MUST be in a transaction for @Lock to work
            System.out.println("Main Thread: Fetching events...");
            long start = System.currentTimeMillis();

            List<Event> result = new org.springframework.transaction.support.TransactionTemplate(transactionManager)
                    .execute(status -> eventRepository.findTop50ByPartitionIdAndStatusOrderByScheduledTime(1,
                            EventStatus.PENDING));

            long duration = System.currentTimeMillis() - start;
            System.out.println("Main Thread: Fetch took " + duration + "ms");

            // Verify fast execution
            assertThat(duration).isLessThan(2000);

            // 2. Result should NOT contain Event 1
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getId()).isEqualTo(event2.getId());

        } finally {
            // Release the background thread so it updates DB and exits
            testCompleteLatch.countDown();
            t1.join();
        }
    }

    private Event createEvent(String name, int partition) {
        Event e = new Event();
        e.setEventName(name);
        e.setPartitionId(partition);
        e.setStatus(EventStatus.PENDING);
        e.setScheduledTime(LocalDateTime.now());
        return e;
    }
}
