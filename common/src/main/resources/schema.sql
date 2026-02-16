CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(36) PRIMARY KEY,
    partition_id INT NOT NULL,
    scheduled_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    event_name VARCHAR(255),
    payload TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    locked_at TIMESTAMP,
    exception_stack_trace TEXT,
    namespace VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_events_partition_status_scheduled 
ON events(partition_id, status, scheduled_time);

CREATE TABLE IF NOT EXISTS outbox (
    id VARCHAR(36) PRIMARY KEY,
    aggregate_id VARCHAR(36) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    payload TEXT,
    created_at TIMESTAMP NOT NULL,
    partition_id INT
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at 
ON outbox(created_at);
