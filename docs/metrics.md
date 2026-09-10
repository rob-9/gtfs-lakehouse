# Metric contracts

All current windows span five minutes of event time and emit a final result after two minutes of accepted lateness. A reported prediction belongs to the window in which the feed observed it. Its scheduled arrival time may be later. Late events cannot revise an already-finalized window in the first implementation; they are audited for a separate replay generation.

The aggregate key is serving generation, agency, route, direction, service date, and window start. Unknown directions use -1. Service dates currently require an explicit trip start date; ambiguous trip instances are retained with an unmatched reason. The local profile supports scheduled trips and one agency per static archive.

## Reported delays

Within a window, select the latest observation by `(observed_at, event_id)` for each trip/stop sample. Count each sample once, even when a feed repeatedly reports it. A cancellation removes predictions for that trip. Missing arrival delay is unknown, not zero. Percentiles use nearest rank. Adherence is the fraction of reported delays between -60 and 300 seconds, inclusive. These are prediction metrics, not measured arrival performance.

## Stop observations

A VehiclePosition with explicit `STOPPED_AT` and a stop ID is a stop observation. Retain the earliest such timestamp per trip/stop within each window. Headways compare consecutive distinct trip observations at the same stop within that window. The current detector does not infer arrivals from GPS proximity and does not bridge window boundaries; sparse feeds can therefore undercount arrivals. No headway is emitted from fewer than two observations.

## Coverage limits

Cancellation counts describe reported trip status within the window. Missing telemetry is not proof of canceled service. Scheduled-trip coverage, cross-window headway deviation, bunching, and inferred observed delay require a schedule-driven service-instance detector and are not yet exposed as complete metrics. The output omits these values rather than inventing denominators.

## Consistency and latency

Final rows have revision 1 in the `live-v1` generation. Serving queries choose the latest row explicitly; they do not depend on ClickHouse background merges. Rebuilds use a fresh generation and must pass parity checks before selection. Iceberg and Kafka outputs commit independently and converge after recovery.

Pipeline visibility latency begins at HTTP body receipt and ends at a successful query probe. It includes window completion and lateness for final results. Source freshness uses observation time and is measured separately. No throughput or latency target has been demonstrated yet.
