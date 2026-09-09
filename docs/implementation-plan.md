# GTFS Streaming Lakehouse implementation plan

## Goal

Build a reproducible streaming system that joins GTFS Realtime updates to versioned GTFS schedules, keeps replayable history in Apache Iceberg, and serves low-latency route metrics from ClickHouse.

The system should demonstrate event-time processing, bounded deduplication, deterministic correction handling, checkpoint recovery, and reproducible performance tests. Throughput and latency numbers remain targets until raw benchmark artifacts, hardware details, and correctness results are checked in.

## Consistency model

The source is an HTTP API, so exactly-once delivery cannot cover the entire path. The guarantees are explicit:

- Polling is at least once. A content-derived `snapshot_id` makes repeated HTTP responses identifiable.
- Normalized events have deterministic IDs and may be delivered more than once through Kafka.
- Flink removes duplicates within a configured event-time horizon and uses checkpoint-aligned, exactly-once sinks for Iceberg and the aggregate changelog.
- ClickHouse is a convergent serving projection. Deterministic aggregate keys and versions make retries and late corrections replace older results.
- Iceberg normalized history is the replay source of truth; raw snapshots are the audit source.

## Data path

```text
GTFS static ZIPs -> schedule loader -> Iceberg schedule snapshots
                                           |
GTFS-RT endpoints -> poller -> Kafka raw + normalized topics -> Flink
                                                               |-- Iceberg history
                                                               `-- Kafka aggregate changelog -> ClickHouse

Iceberg replay -> shared transformations -> batch oracle / ClickHouse rebuild
```

The poller owns fetching, protobuf decoding, envelope normalization, and dead-letter publication. Flink owns event-time validation, deduplication, schedule enrichment, windows, and durable sinks. This removes the earlier ambiguity about which component performs normalization.

## Contracts

Every normalized entity uses an immutable envelope containing:

- `event_id`: SHA-256 of a canonical tuple containing agency, feed, entity type, entity ID, observation time, and payload hash. Entity IDs are feed-scoped; cross-feed vehicle reconciliation is a separate enrichment concern.
- `agency_id`, `feed_id`, `entity_type`, `entity_id`, `trip_id`, `route_id`, and optional `vehicle_id`.
- `observed_at`, `feed_generated_at`, and `ingested_at`, all UTC epoch milliseconds.
- `timestamp_source`, which records whether event time came from the entity or feed header.
- Optional `schedule_version`, populated only after enrichment.
- Typed `payload` and a SHA-256 `payload_hash` calculated from canonical JSON. The payload includes the trip descriptor (trip, route, direction, start date/time, and schedule relationship) and vehicle ID so corrections cannot collide with previous observations.

GTFS-RT timestamps are not uniformly present. Vehicle timestamps are preferred; otherwise the feed-header timestamp is used and marked. Messages without either timestamp are rejected. Pipeline latency and source freshness are reported separately.

Static feed versions are content-addressed. Operators must provide an `effective_from` instant for each archive; it is not inferred from service calendars. A later version closes the previous version's range only after its Iceberg transaction commits.

Feed headers and protobuf required fields are validated before normalization. Differential feeds are rejected until their state and deletion semantics are implemented. Raw snapshot persistence and Kafka publication remain pending; polling currently returns records to its caller. The caller must persist a snapshot and acknowledge its outputs before adopting returned HTTP validators, otherwise a crash followed by HTTP 304 could skip unpersisted data.

## Metric and replay semantics

TripUpdate arrival times and delays may be predictions. Label these as reported predictions; do not count them as observed stop arrivals. Derive observed headway only from an explicit stop-arrival detector with golden fixtures and documented tolerances. Define missing service against scheduled trips after a grace period, and distinguish missing telemetry from confirmed canceled service. Follow the [GTFS Realtime reference](https://old.gtfs.org/realtime/reference/) for trip-instance and cancellation semantics.

Resolve service dates in the agency timezone using trip start dates where supplied; schedule times beyond 24:00 belong to the same service day. Include overnight and daylight-saving fixtures before implementing schedule joins. Declare frequency-based trips unsupported until the loader handles `frequencies.txt`.

Define the watermark allowance, accepted-lateness horizon, and deduplication retention together. Events beyond that horizon go to a late-event audit stream and can be included in a separate replay. A processing-time TTL alone cannot establish deterministic event-time deduplication.

The initial aggregate implementation emits one final row per closed window. Late revisions require a checkpointed revision counter per key and a single writer; input count alone is not a safe version across independent runs. Rebuild into a separate serving generation, validate parity, then switch the serving view. Compare final keys, values, and canonical input digests with the oracle, not arrival-order-dependent revision counters. The route metric schema remains provisional until these semantics are implemented.

Independent Iceberg tables and Kafka transactions are not one atomic cross-system transaction. Recovery tests must cover failure between sink commits and verify eventual parity, rather than promise atomic visibility across stores.

## Topics and storage

Kafka topics:

- `gtfs.raw.snapshots`: immutable protobuf bytes plus HTTP metadata, keyed by feed.
- `gtfs.normalized.events`: normalized envelopes, keyed by agency/entity type/entity ID.
- `gtfs.schedule.versions`: compacted, committed schedule-version metadata.
- `gtfs.route.metrics`: deterministic route-window changelog.
- `gtfs.dead_letter`: structured failures with source and reason.

Start with 24 normalized-event partitions in benchmark profiles; use fewer in the laptop profile. Producers use `acks=all`, idempotence, compression, and bounded retries.

Iceberg v2 tables hold raw snapshots, normalized events, enriched vehicle events, schedule metadata and entities, and route-window metrics. Audit tables are append-only. Maintenance preserves snapshots referenced by active jobs and benchmark reports.

ClickHouse consumes versioned route-window rows into a staging table and exposes the latest version per agency, route, direction, service date, and window start. Rebuilds always originate from a named Iceberg snapshot.

## Delivery sequence

### 1. Executable contracts

- Add Avro schemas, fixture builders, and canonical hashing tests.
- Implement conditional HTTP polling and raw snapshot records.
- Normalize VehiclePosition and TripUpdate entities with explicit rejection reasons.
- Run unit tests without containers.

Acceptance: ingesting the same fixture twice yields byte-for-byte stable identity fields, while a payload or observation-time change yields a new event ID.

### 2. Small local vertical slice

- Add pinned Compose services for Kafka, Flink, MinIO, an Iceberg REST catalog, ClickHouse, Prometheus, and Grafana.
- Create topics and tables with idempotent bootstrap jobs.
- Publish one fixture, deduplicate and enrich it, and query one route metric.
- Provide `make up`, `make smoke`, and `make down`.

Acceptance: a clean checkout reaches healthy state and the smoke test verifies the result, not merely process liveness.

### 3. Versioned schedule enrichment

- Validate and load stops, routes, trips, stop times, calendars, and calendar dates.
- Select the configured schedule version valid at `observed_at`.
- Add bounded-out-of-orderness watermarks, idle partition handling, stable operator UIDs, and deduplication TTL.
- Preserve unmatched events with a reason instead of silently dropping them.

Acceptance: late, out-of-order, duplicate, version-boundary, and missing-trip fixtures produce a deterministic golden stream.

### 4. Durable analytics

- Commit normalized and enriched records to Iceberg on checkpoints.
- Calculate delay, adherence, observed headway, missing service, and route reliability.
- Materialize versioned aggregates in ClickHouse and document them with dbt.
- Add a minimal operational dashboard.

Acceptance: rebuilding ClickHouse from a fixed Iceberg snapshot matches live route-window results.

### 5. Recovery and replay

- Inject TaskManager, Kafka consumer, and ClickHouse restarts during checkpoints.
- Inject corrupt protobufs, hot keys, missing references, duplicates, and events beyond allowed lateness.
- Run canonical inputs through both streaming transforms and a deterministic batch oracle.

Acceptance: acknowledged source snapshots remain recoverable, accepted aggregate sets match, and every intentional rejection has a dead-letter record.

### 6. Benchmarking

- Add deterministic steady, burst, disorder, duplicate, hot-agency, replay, and failure scenarios.
- Capture hardware, image digests, JVM settings, parallelism, partitions, checkpoint configuration, event sizes, and compression.
- Warm up, repeat each scenario at least three times, and publish median plus variance.

Acceptance: reports include throughput; p50, p95, and p99 ingestion-to-query latency; source freshness; watermark and Kafka lag; checkpoint duration; recovery time; resource use; and aggregate parity.

## Guardrails

- Live and replay paths share transformation code or golden fixtures.
- Schema compatibility is checked in CI before deployment.
- No benchmark claim appears in the README until its complete evidence is committed.
- One billion events is a benchmark profile, not a normal test prerequisite.
- Image tags are pinned for development; benchmark reports also capture immutable digests.
