CREATE TABLE IF NOT EXISTS rides (
    ride_id         TEXT PRIMARY KEY,
    rider_id        TEXT NOT NULL,
    route           TEXT NOT NULL,
    fare            NUMERIC(10,2) NOT NULL,
    status          TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
