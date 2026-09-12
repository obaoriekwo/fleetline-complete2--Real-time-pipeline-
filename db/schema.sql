-- Schema for the `rideshare` database (serving/aggregation layer).
-- Raw events live in S3 (data lake); this schema holds curated,
-- query-friendly aggregates that Spark Streaming writes on each micro-batch.
--
-- Apply with:
--   psql -h localhost -p 5432 -U postgres -d rideshare -f db/schema.sql

CREATE TABLE IF NOT EXISTS trips (
    trip_id         UUID PRIMARY KEY,
    rider_id        VARCHAR(64) NOT NULL,
    driver_id       VARCHAR(64),
    city            VARCHAR(64) NOT NULL,
    status          VARCHAR(24) NOT NULL,   -- requested | accepted | in_progress | completed | cancelled
    pickup_lat      DOUBLE PRECISION,
    pickup_lng      DOUBLE PRECISION,
    dropoff_lat     DOUBLE PRECISION,
    dropoff_lng     DOUBLE PRECISION,
    fare            NUMERIC(10, 2),
    requested_at    TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trips_status ON trips (status);
CREATE INDEX IF NOT EXISTS idx_trips_city ON trips (city);
CREATE INDEX IF NOT EXISTS idx_trips_requested_at ON trips (requested_at);

-- Latest known position per driver (upserted, not a time series)
CREATE TABLE IF NOT EXISTS driver_locations (
    driver_id       VARCHAR(64) PRIMARY KEY,
    city            VARCHAR(64) NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,
    lng             DOUBLE PRECISION NOT NULL,
    status          VARCHAR(24) NOT NULL,   -- available | en_route | on_trip | offline
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Windowed aggregates produced by Spark Streaming (one row per minute per city)
CREATE TABLE IF NOT EXISTS metrics_minute (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    city                 VARCHAR(64) NOT NULL,
    events_total          INTEGER NOT NULL DEFAULT 0,
    trips_requested       INTEGER NOT NULL DEFAULT 0,
    trips_completed       INTEGER NOT NULL DEFAULT 0,
    trips_cancelled       INTEGER NOT NULL DEFAULT 0,
    avg_fare              NUMERIC(10, 2),
    avg_eta_seconds        INTEGER,
    PRIMARY KEY (window_start, city)
);

CREATE INDEX IF NOT EXISTS idx_metrics_minute_window ON metrics_minute (window_start);

-- Pipeline health / uptime tracking (used for the 99.9% uptime + failover story)
CREATE TABLE IF NOT EXISTS pipeline_health (
    component            VARCHAR(64) PRIMARY KEY,   -- kafka | spark_streaming | emr_cluster | api
    status                VARCHAR(24) NOT NULL,       -- healthy | degraded | down
    last_heartbeat        TIMESTAMPTZ NOT NULL,
    uptime_pct_24h        NUMERIC(5, 2),
    details               JSONB
);

-- Raw event log, optional — useful for debugging/replay without going back to S3
CREATE TABLE IF NOT EXISTS trip_events_log (
    event_id             BIGSERIAL PRIMARY KEY,
    trip_id              UUID,
    event_type            VARCHAR(32) NOT NULL,
    event_ts              TIMESTAMPTZ NOT NULL,
    payload               JSONB,
    ingested_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trip_events_log_ts ON trip_events_log (event_ts);
