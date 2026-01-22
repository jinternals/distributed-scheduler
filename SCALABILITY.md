# Scalability & Concurrency Design

This document details how the Distributed Scheduler handles high concurrency and large volumes of events, along with current limitations.

## 1. Horizontal Scaling (Cluster Level)
The primary mechanism for handling high loads is **Apache Helix Partitioning**.

*   **Partitioning**: The "Event Space" is divided into logical partitions (e.g., 6 partitions).
*   **Distribution**: Each Worker Node is assigned a subset of these partitions.
*   **Scaling**: To handle more load, you can simply:
    1.  Add more Worker Nodes. Helix automatically rebalances partitions to the new nodes.
    2.  Increase the number of Partitions (requires DB migration strategy) to allow for more granular distribution.

## 2. Concurrency & Locking (Database Level)
We use a robust locking strategy to ensure data consistency without performance penalties.

*   **`SKIP LOCKED`**: The query `findTop50...` uses `UPGRADE_SKIPLOCKED`.
    *   **Benefit**: This allows the database to skip over rows that are currently locked by other transactions. Even if multiple threads/workers accidentally try to poll the same data, they won't block each other; they will just grab different available rows.
*   **Granular Transactions**:
    *   Events are fetched in a batch (Efficiency).
    *   Events are processed and committed **individually** (Safety).
    *   **Benefit**: If processing one event takes time or fails, it doesn't hold the database lock for the entire batch. This minimizes deadlocks and contention.

## 3. Throughput Limiting Factors (Node Level)
While the cluster scales well, individual nodes have limitations in the current implementation:

### A. Synchronous Processing (Resolved)
The `EventProcessor` now uses a **Thread Pool** to process events in parallel:
*   It fetches 50 events.
*   It processes them **in parallel** using an `ExecutorService`.
*   **Impact**: Throughput of a single node is significantly increased. If an event takes 100ms, 50 events can be processed in parallel (limited by pool size), effectively reducing batch processing time.
*   **Implementation**: Uses `CompletableFuture.runAsync()` with a `ThreadPoolTaskExecutor`.

### B. Partition Starvation (Mitigated)
The polling logic now implements **Round-Robin** across assigned partitions to prevent starvation:
```java
Collections.shuffle(activePartitions);
while (!activePartitions.isEmpty()) {
    // Round-robin polling
}
```
*   **Mechanism**: The processor iterates through all assigned partitions. If a partition has work (batch of 50), it processes it. If a partition is empty, it is removed from the cycle.
*   **Result**: Even if Partition 0 has a massive surge, the processor will still give a "turn" to Partition 1, 2, etc., ensuring fair distribution.

## 4. Summary
*   **High Event Volume**: Handled well by adding workers (Horizontal Scaling).
*   **High Contention**: Handled by `SKIP LOCKED` and Granular Transactions.
*   **Latency**: Reduced by parallel processing.

## 5. Recommended Improvements
1.  **Parallel Processing**: Implemented thread pool for `handleEvent`.
2.  **Fairness**: `EventProcessor` now uses round-robin to prevent partition starvation (Implemented).
