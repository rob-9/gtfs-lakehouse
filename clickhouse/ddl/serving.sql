CREATE DATABASE IF NOT EXISTS gtfs;

CREATE TABLE IF NOT EXISTS gtfs.route_metrics
(
    generation String,
    agency_id String,
    route_id String,
    direction_id Int32,
    service_date Date,
    window_start Int64,
    version UInt64,
    record_json String,
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (generation, agency_id, route_id, direction_id, service_date, window_start);

CREATE VIEW IF NOT EXISTS gtfs.route_metrics_latest AS
SELECT generation, agency_id, route_id, direction_id, service_date, window_start,
       argMax(record_json, route_metrics.version) AS record_json,
       max(route_metrics.version) AS version
FROM gtfs.route_metrics
GROUP BY generation, agency_id, route_id, direction_id, service_date, window_start;
