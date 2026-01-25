# Distributed Scheduler with Apache Helix

A robust, distributed task scheduler system built with Java, Spring Boot, Apache Helix, and Zookeeper. This project demonstrates how to build a scalable worker cluster where tasks are partitioned and processed efficiently across multiple nodes.

## 🚀 Features

*   **Distributed Architecture**: Leverages Apache Helix for cluster management and partition assignment.
*   **Dynamic Scaling**: Worker nodes automatically join the cluster, and partitions are rebalanced on the fly.
*   **Fault Tolerance**: If a worker node fails, its partitions are reassigned to other available workers.
*   **Task Partitioning**: Tasks are sharded using consistent hashing to ensure varied distribution and load balancing.
*   **Active Polling**: Workers actively poll for pending tasks in their assigned partitions.
*   **Spring Boot**: Built on modern Spring Boot 3 microservices.
*   **Dockerized**: Fully containerized setup with Docker Compose for easy deployment.

## 🏗️ Architecture

The system consists of the following components:

1.  **Scheduler API (`scheduler-api`)**:
    *   Exposes REST endpoints to create (`POST /tasks`) and delete tasks.
    *   Persists task metadata to a PostgreSQL database.
    *   Stateless and horizontally scalable.

2.  **Worker Node (`worker-node`)**:
    *   Helix Participant that joins the `scheduler-cluster`.
    *   Receives partition assignments from the Helix Controller.
    *   Periodically polls the database for `PENDING` tasks belonging to its assigned partitions.
    *   Processes tasks and updates their status to `COMPLETED`.

3.  **Helix Controller (`helix-controller`)**:
    *   Manages the state of the cluster.
    *   Handles node failures and rebalances partitions.

4.  **Cluster Init (`cluster-init`)**:
    *   A one-off job that initializes the Helix cluster structure and configurations in Zookeeper.

5.  **Infrastructure**:
    *   **Zookeeper**: Coordination service for Apache Helix.
    *   **PostgreSQL**: Persistent storage for tasks/events.

## 🛠️ Prerequisites

*   **Java 17+**
*   **Docker** & **Docker Compose**
*   **Maven** (optional, for local development)

## 🏁 Getting Started

### 1. clone the repository
```bash
git clone https://github.com/jinternals/distributed-scheduler.git
cd distributed-scheduler
```

### 2. Start the System
Build and start all services using Docker Compose:
```bash
docker-compose up -d --build
```
*Note: The first run might take a few minutes to download dependencies and build the JARs.*

### 3. Verify Cluster Status
You can verify that the cluster is up and workers have joined by checking the logs:
```bash
docker-compose logs -f worker-node
```
You should see logs indicating:
> "Polling events for partitions: [0, 1, 2, ...]"

### 4. Scale Workers
To see the distributed nature in action, scale up the worker nodes:
```bash
docker-compose up -d --scale worker-node=3
```
Apache Helix will automatically detect the new nodes and redistribute the partitions.

## 📡 API Usage

### Create a Task
Schedule a new task. The system will assign it a `partitionId` and store it.

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "MyDistributedTask",
    "scheduledTime": "2023-12-31T23:59:59",
    "payload": "Payload Data"
  }'
```

**Response:**
```json
{
  "id": 1,
  "eventName": "MyDistributedTask",
  "status": "PENDING",
  "partitionId": 5
}
```

### Delete a Task
```bash
curl -X DELETE http://localhost:8080/tasks/1
```

## 🧪 Verification

### Verify Distribution
1.  Run 3 worker nodes.
2.  Submit multiple tasks (e.g., 10).
3.  Check the logs of each worker container. You will see different workers picking up different tasks based on the `partitionId`.

```bash
# Example: Check which worker processed Task ID 5
docker-compose logs worker-node | grep "Handling event"
```

## 🤝 Contributing
Contributions are welcome! Please fork the repository and submit a Pull Request.

## 📄 License
This project is licensed under the MIT License.
