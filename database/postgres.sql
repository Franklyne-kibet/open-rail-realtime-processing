CREATE TABLE train_schedule (
    unique_id TEXT ,
    location_code TEXT,
    schedule_id BIGINT,
    actual_arrival TIMESTAMP,
    actual_departure TIMESTAMP,
    estimated_time TIMESTAMP,
    platforms TEXT,
    scheduled_arrival TIMESTAMP,
    scheduled_departure TIMESTAMP,
    sequence_number BIGINT,
    service_start_date DATE,
    source TEXT
);