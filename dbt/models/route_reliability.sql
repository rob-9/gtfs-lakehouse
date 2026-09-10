SELECT generation, agency_id, route_id, direction_id, service_date, window_start,
       JSONExtractInt(record_json, 'prediction_count') AS prediction_count,
       JSONExtract(record_json, 'mean_delay_seconds', 'Nullable(Float64)') AS mean_delay_seconds,
       JSONExtract(record_json, 'p95_delay_seconds', 'Nullable(Float64)') AS p95_delay_seconds,
       JSONExtract(record_json, 'adherence_rate', 'Nullable(Float64)') AS adherence_rate,
       JSONExtractInt(record_json, 'canceled_trip_count') AS canceled_trip_count
FROM gtfs.route_metrics_latest
