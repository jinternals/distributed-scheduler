package com.jinternals.scheduler.workernode.service;

import com.jinternals.scheduler.common.model.OutboxEvent;
import com.jinternals.scheduler.common.repositories.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Service
@Profile("!init & !controller")
public class OutboxPublisher {

    private static final Logger logger = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int POLL_INTERVAL_MS = 1000;

    private final PartitionManager partitionManager;
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Executor eventTaskExecutor;
    private final TransactionTemplate transactionTemplate;

    public OutboxPublisher(PartitionManager partitionManager,
            OutboxRepository outboxRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier("eventTaskExecutor") Executor eventTaskExecutor,
            PlatformTransactionManager transactionManager) {
        this.partitionManager = partitionManager;
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.eventTaskExecutor = eventTaskExecutor;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelay = POLL_INTERVAL_MS)
    public void execute() {
        Set<Integer> partitions = partitionManager.getActivePartitions();
        if (partitions.isEmpty()) {
            return;
        }

        List<Integer> activePartitions = new LinkedList<>(partitions);
        Collections.shuffle(activePartitions);

        for (Integer partition : activePartitions) {
            publishPartitionBatch(partition);
        }
    }

    private void publishPartitionBatch(Integer partition) {
        eventTaskExecutor.execute(() -> {
            int batchCount = 0;
            // Limit batches to avoid starving others, similar to EventProcessor
            while (batchCount < 10) {
                boolean processed = processBatch(partition);
                if (!processed) {
                    break;
                }
                batchCount++;
            }
        });
    }

    private boolean processBatch(Integer partition) {
        // Fetch batch
        List<OutboxEvent> events = outboxRepository.findTop500ByPartitionIdOrderByCreatedAt(partition);
        if (events.isEmpty()) {
            return false;
        }

        // Send to Kafka Async
        List<CompletableFuture<Void>> futures = events.stream()
                .map(this::sendToKafka)
                .toList();

        try {
            // Wait for all to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Delete successful events
            // In a real system, you might want to handle partial failures more gracefully,
            // but for now, we assume if sendToKafka doesn't throw, it succeeded.
            // (The sendToKafka below handles exceptions by logging and not throwing to
            // allow the join to complete)
            // However, we should only delete what succeeded.
            // For simplicity in this phase: Delete all if simple join succeeds.
            // If join throws (one failed), the transaction rollback (if wrapped) or we
            // retry all.
            // But we are not in a transaction here (intentionally, to keep DB locks short).
            // So we can assume manual cleanup.

            outboxRepository.deleteAllById(events.stream().map(OutboxEvent::getId).collect(Collectors.toList()));

            logger.info("Published and deleted {} outbox events for partition {}", events.size(), partition);
            return true;

        } catch (Exception e) {
            logger.error("Error publishing batch for partition " + partition, e);
            return false;
        }
    }

    private CompletableFuture<Void> sendToKafka(OutboxEvent event) {
        return kafkaTemplate.send("scheduler-events", event.getAggregateId(), event.getPayload())
                .thenAccept(result -> {
                    // Success callback if needed
                })
                .exceptionally(ex -> {
                    logger.error("Failed to publish event {}", event.getId(), ex);
                    throw new RuntimeException(ex);
                });
    }
}
