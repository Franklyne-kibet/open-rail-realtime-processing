CREATE KEYSPACE IF NOT EXISTS train_service WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};
USE train_service;

CREATE TABLE IF NOT EXISTS service_performance (
    "sequence_number" bigint,
    "schedule_id" bigint,
    "unique_id" text,
    "service_start_date" date,
    "location_code" text,
    "scheduled_arrival" time,
    "scheduled_departure" time,
    "actual_arrival" time,
    "actual_departure" time,
    "platforms" text,
    "estimated_time" time,
    "source" text,
    PRIMARY KEY (("unique_id", "location_code"), "schedule_id")
);