# Scalability & Concurrency Design

This document details how the Distributed Scheduler handles high concurrency and large volumes of events.

## 1. Horizontal Scaling (Cluster Level)
The primary mechanism for handling high loads is **Apache Helix Partitioning**.

*   **Partitioning**: The "Event Space" is divided into logical partitions.
*   **Dynamic Configuration**: The number of partitions is configurable via `scheduler.partitions` (e.g., 12) in `application.properties`.
*   **Distribution**: Each Worker Node is assigned a subset of these partitions efficiently by the Helix Controller.
*   **Scaling**:
    1.  **Add Nodes**: Helix automatically rebalances partitions to the new nodes (e.g., if you have 12 partitions and 6 nodes, each node gets 2).
    2.  **Increase Partitions**: Increase `scheduler.partitions` to allow for even more granular distribution across a larger fleet.

## 2. Concurrency & Locking (Database Level)
We use a robust locking strategy to ensure data consistency without performance penalties.

*   **`SKIP LOCKED`**: The query `findTop50...` uses `UPGRADE_SKIPLOCKED`.
    *   **Benefit**: This allows the database to skip over rows that are currently locked by other transactions. Multiple workers can poll the same table without blocking each other.
*   **Granular Transactions**:
    *   Events are fetched in a batch (Efficiency).
    *   Events are processed and committed **individually** (Safety).
    *   **Benefit**: Minimizes deadlocks and lock contention duration.

## 3. Throughput Optimizations (Node Level)

### A. Parallel Processing
The `EventProcessor` uses a dedicated **Thread Pool** to process events in parallel, preventing a single slow event from blocking the entire batch.

*   **Implementation**: `CompletableFuture.supplyAsync()` using a custom `Executor` bean.
*   **Configuration**: Defined in `ExecutorConfiguration.java`.
    *   **Core Pool Size**: 10 threads.
    *   **Max Pool Size**: 50 threads.
    *   **Queue Capacity**: 100 tasks.
*   **Impact**: Significantly increases per-node throughput, especially for I/O-bound tasks.

### B. Round-Robin Polling
To prevent "Partition Starvation" (where one busy partition consumes all resources), the processor implements `Collections.shuffle` and a Round-Robin iterator to fairly service all assigned partitions in each cycle.

## 4. Current Configuration Status
1.  **Partitioning**: Dynamic (default: 6, tested: 12).
2.  **Latency Simulation**: The current codebase includes `Thread.sleep(200)` in `EventProcessor` to simulate realistic high-latency work (CPU/IO).
3.  **Audit**: `Event` entity tracks `createdAt` and `updatedAt`.
