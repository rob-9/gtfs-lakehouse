SELECT generation, agency_id, route_id, direction_id, service_date, window_start,
       JSONExtractInt(record_json, 'prediction_count') AS prediction_count,
       JSONExtract(record_json, 'mean_delay_seconds', 'Nullable(Float64)') AS mean_delay_seconds,
       JSONExtract(record_json, 'p95_delay_seconds', 'Nullable(Float64)') AS p95_delay_seconds,
       JSONExtract(record_json, 'adherence_rate', 'Nullable(Float64)') AS adherence_rate,
       JSONExtractInt(record_json, 'canceled_trip_count') AS canceled_trip_count,
       JSONExtractInt(record_json, 'arrival_count') AS arrival_count,
       JSONExtractInt(record_json, 'headway_count') AS headway_count,
       JSONExtract(record_json, 'mean_headway_seconds', 'Nullable(Float64)') AS mean_headway_seconds
FROM gtfs.route_metrics_latest
