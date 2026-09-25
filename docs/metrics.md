# Metric contracts

All current windows span five minutes of event time and emit a final result after two minutes of accepted lateness. A reported prediction belongs to the window in which the feed observed it. Its scheduled arrival time may be later. Late events cannot revise an already-finalized window in the first implementation; they are audited for a separate replay generation.

The aggregate key is serving generation, agency, route, direction, service date, and window start. Unknown directions use -1. Service dates currently require an explicit trip start date; ambiguous trip instances are retained with an unmatched reason. The local profile supports scheduled trips and one agency per static archive.

## Reported delays

Within a window, select the latest observation by `(observed_at, event_id)` for each trip/stop sample. Count each sample once, even when a feed repeatedly reports it. A cancellation removes predictions for that trip. Missing arrival delay is unknown, not zero. Percentiles use nearest rank. Adherence is the fraction of reported delays between -60 and 300 seconds, inclusive. These are prediction metrics, not measured arrival performance.

## Stop observations

In the legacy `live-v2` generation, a VehiclePosition with explicit `STOPPED_AT` and a stop ID is a stop observation. Retain the earliest such timestamp per trip/stop within each window. Headways compare consecutive distinct trip observations at the same stop within that window. The current detector does not infer arrivals from GPS proximity and does not bridge window boundaries; sparse feeds can therefore undercount arrivals. No headway is emitted from fewer than two observations.

## Coverage limits

Cancellation counts describe reported trip status within the window. Missing telemetry is not proof of canceled service. Scheduled-trip telemetry coverage is available as a bounded, pinned report (below). Headway deviation, bunching, and inferred observed delay are not yet exposed as complete metrics. The output omits these values rather than inventing denominators.

## Consistency and latency

Final rows have revision 1 in the legacy `live-v2` and cross-window `live-v3` generations. Serving queries choose the latest row explicitly; they do not depend on ClickHouse background merges. Rebuilds use a fresh generation and must pass parity checks before selection. Iceberg and Kafka outputs commit independently and converge after recovery.

Both deduplication and window admission persist their watermark cutoff. This prevents recovery from reopening old windows before source watermarks are re-established, a known [Flink recovery consideration](https://issues.apache.org/jira/browse/FLINK-5601). The `metric_inputs` table records the exact admitted records used by the window operator; replay uses this ledger rather than treating every enriched record as an accepted metric input.

The earlier development generation `live-v1` is retained for auditing. It exposed a provenance mismatch after restart and is not covered by `live-v2` parity reports. New generation defaults keep those historical results separate without deleting source history.

Pipeline visibility latency begins at HTTP body receipt and ends at a successful query probe. It includes window completion and lateness for final results. Source freshness uses observation time and is measured separately. No throughput or latency target has been demonstrated yet.

## Cross-window observed headways (`live-v3`)

The `live-v3` route operator retains first explicit `STOPPED_AT` visits by trip and stop across five-minute boundaries, keyed by agency, route, direction, and service date. It finalizes windows in event-time order after the same two-minute lateness allowance. For each new visit, compare the latest preceding distinct trip at that stop, including earlier windows, and assign the headway to the later visit's window. Equal timestamps are ordered by event ID and may produce a zero headway. Repeated observations of the same trip/stop within the retained horizon are dwell reports and do not add arrivals or headways.

The lookback is two hours, including the boundary; longer gaps do not produce headways. A stop visit first seen more than two hours earlier may be counted again. No comparison crosses route, direction, service-date, or stop boundaries. This remains an explicit-stop detector, not GPS arrival inference, and it does not distinguish repeated visits to the same stop on loop trips. Sparse telemetry can miss visits. State is fixture-scale and retained by event time; when watermarks stop, finalization and cleanup also stop.

The operator checkpoints pending windows, retained visits, and the admission cutoff. Every admitted event is persisted in `metric_inputs` with generation `live-v3`. Aggregate schema version 2 adds `context_event_count` and `headway_lookback_seconds`; the input digest covers current window events plus all retained predecessor-context event IDs. An independent Python oracle replays all pinned closed windows in order and checks the context and final values. Replay requires the original admission ledger; per-window legacy backfills are not safe for this generation.

The original `live-v2` operator and its state remain in the graph for compatible savepoint restores and legacy replay. Both generations are served and stored separately. On the first upgrade, `live-v3` starts from future inputs at the restored source position, with no predecessor context until it observes visits; it does not backfill historical windows. Its first headways therefore have a warm-up boundary. Default queries remain on `live-v2`; select `live-v3` explicitly for cross-window results. Retiring the legacy operator requires a separate state migration.

## Scheduled-trip telemetry coverage

`coverage` enumerates every calendar-active trip in an explicitly selected schedule version and service date, including trips with no realtime records. It uses GTFS service time in the agency timezone, including hours beyond 24:00 and daylight-saving transitions. A trip becomes due at its latest scheduled stop departure plus a configurable grace period (default five minutes). Trips before that instant are `not_due` and do not enter the due-service denominator.

For due trips, evidence is restricted to the same agency, service date, and schedule version, with observation time at or before the requested as-of instant. Deduplicate by event ID, then process observations in `(observed_at, event_id)` order. A reported cancellation classifies a trip as `canceled`; only a subsequent TripUpdate reinstates it. A VehiclePosition does not clear a cancellation. Other trips with TripUpdate or VehiclePosition evidence are `telemetry_present`; those without evidence are `missing_telemetry`. Predictions prove telemetry presence, not that service operated. The categories are mutually exclusive, and missing telemetry never implies cancellation.

Per-route/direction summaries expose scheduled, due, not-due, telemetry-present, canceled, and missing-telemetry counts. `telemetry_coverage_rate` is telemetry-present due trips divided by due trips minus reported cancellations; it is null when that denominator is zero. Trip details retain due times, evidence counts, and input digests. The report identifies the exact schedule version and digest, pinned input snapshots, as-of time, grace period, and excluded evidence counts.

This is a bounded report over a named archive, not a continuously finalized streaming metric or an automatic selection of effective schedule versions. A report cannot use an as-of time later than its snapshot collection. Snapshot pinning does not prove ingestion completeness: later ingestion or a newer snapshot may change missing-telemetry classifications. The report uses enriched history (including events not admitted to route windows) because coverage concerns a whole service date. Events enriched against other schedule versions are excluded and counted; choose the appropriate version explicitly rather than treating mixed versions as one denominator.
