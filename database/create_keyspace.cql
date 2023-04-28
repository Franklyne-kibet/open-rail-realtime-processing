CREATE KEYSPACE IF NOT EXISTS train_service WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;

USE train_service;

CREATE TABLE IF NOT EXISTS train_schedule (
    "sequence_number" text,
    "schedule_id" text,
    "unique_id" text,
    "service_start_date" text,
    "location_code" text,
    "scheduled_arrival" text,
    "scheduled_departure" text,
    "actual_arrival" text,
    "actual_departure" text,
    "platforms" text,
    "estimated_time" text,
    "source" text,
    PRIMARY KEY (("unique_id", "location_code"), "schedule_id")
);