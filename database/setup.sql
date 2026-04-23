-- Create the database (run this separately first)
-- CREATE DATABASE geotransit;

-- Trip table: one row per bus journey
CREATE TABLE IF NOT EXISTS trip (
    trip_id       VARCHAR(50) PRIMARY KEY,
    route_id      VARCHAR(20),
    vehicle_id    VARCHAR(20),
    direction     VARCHAR(10),
    created_at    TIMESTAMP DEFAULT NOW()
);

-- Breadcrumb table: every GPS ping from every bus
CREATE TABLE IF NOT EXISTS breadcrumb (
    breadcrumb_id SERIAL PRIMARY KEY,
    trip_id       VARCHAR(50) REFERENCES trip(trip_id),
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    speed         DOUBLE PRECISION,         -- we calculate this
    heading       DOUBLE PRECISION,
    tstamp        TIMESTAMP NOT NULL,
    odometer      DOUBLE PRECISION          -- distance traveled (meters)
);

-- Index for fast time-based queries (same as you'd need in production)
CREATE INDEX IF NOT EXISTS idx_breadcrumb_tstamp ON breadcrumb(tstamp);
CREATE INDEX IF NOT EXISTS idx_breadcrumb_trip ON breadcrumb(trip_id);