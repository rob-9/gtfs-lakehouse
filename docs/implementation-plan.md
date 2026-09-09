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

- `gtfs.raw.snapshots`: immutable manifests with object-store references, body checksums, and HTTP metadata, keyed by agency/feed. Protobuf bodies are persisted in MinIO before publication.
- `gtfs.normalized.events`: normalized envelopes, keyed by agency/feed/entity type/entity ID.
- `gtfs.schedule.versions`: compacted, committed schedule-version metadata.
- `gtfs.route.metrics`: deterministic route-window changelog.
- `gtfs.dead_letter`: structured failures with source and reason.

Start with 24 normalized-event partitions in benchmark profiles; use fewer in the laptop profile. Producers use `acks=all`, idempotence, compression, and bounded retries.

Iceberg v2 tables hold raw snapshot manifests, normalized events, enriched observations, schedule metadata and entities, and route-window metrics. Raw bodies remain in object storage. Audit tables are append-only. Maintenance preserves snapshots and bodies referenced by active jobs and benchmark reports.

ClickHouse consumes versioned route-window rows into a staging table and exposes the latest version per agency, route, direction, service date, and window start. Rebuilds always originate from a named Iceberg snapshot.

## Delivery sequence

### Current baseline

Implemented: conditional HTTP fetching, VehiclePosition and TripUpdate normalization, correction-sensitive event IDs, structured validation failures, Avro schemas, and 27 unit/serialization tests. The fetch function returns records; there is no running service, publisher, persistent store, or streaming job yet.

Work through the phases below in order. Each phase produces a runnable result before the next starts. Start with one synthetic agency and a fixture HTTP server, then expand to configured public feeds. External deployment and remote repository changes are outside this plan.

### 1. Local infrastructure and fixtures

Deliverables:

- Add `compose.yaml` with Kafka, MinIO, an Iceberg REST catalog with persistent metadata, ClickHouse, and Flink JobManager/TaskManager services. Put Prometheus and Grafana in an optional observability profile initially.
- Verify a compatible Java/Flink/Kafka/Iceberg connector combination in a minimal build before pinning versions. Record the tested combination in `docs/development.md`; do not choose each connector independently by newest version.
- Add persistent named volumes, health checks, resource limits, localhost port bindings, and idempotent bucket/topic bootstrap tasks. A laptop broker uses replication factor one and is explicitly not a high-availability deployment.
- Add a fixture HTTP server plus a tiny static GTFS dataset covering two trips, multiple stops, and a cancellation. Generate reproducible ZIP/protobuf fixtures from readable sources.
- Add `make up`, `make down`, `make status`, and `make smoke-platform`. Normal shutdown preserves volumes; deletion requires a separate explicit command.

Acceptance: from a clean checkout, the services become healthy, an object survives a restart, the catalog responds, ClickHouse executes a query, and a fixture event can be published to Kafka and consumed. This phase does not claim a working analytics pipeline.

Suggested commits: `chore: add local data services`, `test: add transit fixtures`, `test: add platform smoke checks`.

### 2. Continuous polling and durable publication

Deliverables:

- Add a CLI and configuration file for agency/feed IDs, URLs, polling intervals, request timeouts, and maximum response sizes. Read authentication from environment variables and redact it from logs and persisted URLs.
- Run feeds concurrently with one in-flight request per feed, bounded concurrency, exponential backoff with jitter, Retry-After handling, and graceful shutdown. A slow or failing feed must not block others.
- Persist each successful response body to MinIO under its content-derived identity before advancing validators. Store a small durable outbox/state database on a local volume, including validators, receipt timestamps, object references, and publication status.
- Publish a raw-snapshot manifest, normalized records, and dead letters with a stable Avro framing contract. Use one Kafka transaction per snapshot with bounded snapshot size and `read_committed` consumers. Give each poller instance a stable, exclusive transactional identity.
- Mark outbox entries complete and advance validators only after Kafka acknowledges the transaction. Replay pending entries after restart. A crash after Kafka commit but before the local state update may produce duplicates, which retain their IDs.
- Store large raw bodies in object storage and put references, checksums, and HTTP metadata in `gtfs.raw.snapshots`; revise its schema and the architecture description accordingly. Do not depend on arbitrary feed bodies fitting Kafka message limits.
- Add `make poll-fixtures` and `make smoke-ingestion`.

Acceptance: a fixture is durably stored and appears in Kafka; a repeated HTTP 304 produces no new events. Restarting at each outbox/publication boundary recovers the snapshot without losing outputs. Malformed feeds produce an auditable record with a raw-object reference. Tests cover 429, timeout, oversized body, corrupt protobuf, and one failing feed alongside one healthy feed.

Suggested commits: `feat: run configured feed pollers`, `feat: persist snapshot outbox`, `feat: publish feed transactions`.

### 3. Static schedules and a reference join

Deliverables:

- Add a schedule-loader CLI that accepts a local ZIP or configured URL, agency ID, and explicit effective-from instant. Hash archives and validate ZIP limits, required columns, unique identifiers, and stop/trip/route references.
- Load agency timezones, routes, stops, trips, stop times, and service calendars. Support `calendar.txt`, `calendar_dates.txt`, or the valid combination of both. Reject unsupported frequency-based schedules explicitly.
- Write immutable version-partitioned schedule data to Iceberg. Because tables commit independently, publish one committed version manifest referencing all table snapshot IDs only after every write succeeds. Consumers ignore incomplete versions.
- Publish the committed manifest to Kafka through a retryable outbox. Re-running a load is idempotent. Derive effective ranges from committed manifests with an explicit policy for overlaps and retroactive versions.
- Implement a small batch reference join with golden expected output. Resolve trip instances, service dates, calendar exceptions, times beyond 24:00, timezone transitions, cancellation status, and explicit unmatched reasons.
- Require realtime ingestion and enrichment to preserve the selected schedule version for subsequent replay.

Acceptance: loading the same archive twice creates one logical version. An interrupted multi-table load is never visible as ready. Golden cases cover midnight, daylight-saving changes, adjacent schedule versions, removed service, canceled trips, and missing references.

Suggested commits: `feat: validate static schedules`, `feat: load versioned schedules`, `test: define schedule join fixtures`.

### 4. Flink event-time processing and Iceberg history

Deliverables:

- Add the Java Maven job, generated Avro bindings, pinned connectors, stable operator UIDs, and a build/deployment command for local Compose.
- Read committed normalized Kafka events, validate event-time bounds, assign watermarks with idle-partition detection, and deduplicate by event ID using event-time cleanup. Define the event age cutoff before evicting deduplication state so old retries cannot become new accepted records.
- Load committed schedule manifests into bounded version-aware lookup state. Start with the fixture-sized schedule set; document its memory limit. Buffer events waiting for a known version within a bounded limit, then audit unresolved events. Retain historical versions needed by allowed lateness and replay.
- Persist the admission decision and selected schedule version. Watermark-dependent rejection can vary with delivery order; replay correctness must use the recorded accepted input set. A replay admitting additional late data is a new result generation.
- Add durable object-store checkpoints and checkpoint-aligned Iceberg sinks for normalized history, enriched observations, and rejection/late-event audit records. Archive raw manifests separately, retaining the referenced bodies in MinIO.
- Clarify logical uniqueness: recovery exactly-once does not remove every upstream duplicate across an unbounded history. Raw history is at least once; the canonical replay dataset uses stable IDs and the recorded admission policy.
- Add `make submit` and `make smoke-enrichment`.

Acceptance: duplicate and out-of-order fixtures produce the golden enriched result; idle partitions do not indefinitely stall watermarks. After a TaskManager restart, committed accepted events remain logically unique and every rejected input is traceable. A fixed checkpoint restores operator state and schedule selection.

Suggested commits: `feat: add Flink event processing`, `feat: join schedule versions`, `feat: persist Iceberg history`.

### 5. Metrics and ClickHouse queries

Deliverables:

- Write `docs/metrics.md` before aggregate code: specify each metric's observation unit, eligibility, grouping key, event clock, window, null handling, and denominator. Repeated predictions for one trip/stop must not be treated as independent arrivals.
- Implement reported delay distributions and schedule adherence first. Separately implement a stop-arrival detector for observed headway, deviation, and bunching, then scheduled-versus-observed service coverage after a configurable grace period. Keep cancellation counts and absent telemetry separate.
- Finalize route/stop aggregate schemas with serving generation, window boundaries, sample counts, input digest, and revision. Start with final windows only; add provisional updates and checkpointed revisions in a separate commit after final-window parity passes.
- Persist final aggregates to Iceberg and emit them to Kafka with checkpointed transactions. Document that these outputs converge after recovery but are not atomically visible together.
- Add a ClickHouse ingestion worker that commits Kafka offsets only after a successful insert. Retried rows must be harmless. Expose latest-row views using explicit key/version selection, without relying on background merges for query correctness; avoid summing correction rows in append-only materialized views.
- Add route reliability and stop delay queries, a query CLI, and `make smoke`. The end-to-end smoke command starts a fresh fixture run, waits for final windows, and checks exact expected query results.

Acceptance: the fixture produces known delay and headway values, duplicate deliveries do not inflate counts, cancellations are classified correctly, and a revised window replaces the old result. Live ClickHouse values match final Iceberg aggregates for the same serving generation.

Suggested commits: `docs: define transit metrics`, `feat: aggregate route reliability`, `feat: serve ClickHouse metrics`, `test: verify end-to-end analytics`.

### 6. Data quality and operational visibility

Deliverables:

- Add dbt-clickhouse models and tests for logical key uniqueness, schedule relationships, sample counts, coordinate validity, and metric denominators. Test deduplicated serving views, since physical staging retries are expected.
- Instrument poll attempts, successes, errors, raw/outbox backlog, feed freshness, Kafka lag, watermarks, deduplication, unmatched events, checkpoint health, sink retries, and query visibility.
- Provision Prometheus and Grafana with one pipeline-health dashboard and one transit dashboard. Add a small runbook for a stalled feed, failed checkpoint, outbox backlog, and lagging sink.
- Measure source freshness separately from ingestion-to-query latency. Closed-window latency includes window duration plus allowed lateness; do not compare it to a two-second provisional-update target. Use external query probes and sampled correlation records instead of calling sink insertion time query visibility.

Acceptance: one injected feed failure and one stopped sink visibly change the appropriate panels and recover after restart. dbt catches a deliberately invalid fixture. Dashboard provisioning works from a clean checkout.

Suggested commits: `feat: add serving quality checks`, `feat: add pipeline dashboards`, `docs: add operations runbook`.

### 7. Replay, rebuilding, and failure recovery

Deliverables:

- Add a replay command accepting named Iceberg snapshot IDs, schedule manifests, an accepted-input policy, a time range, and a new serving generation. Use a manifest to pin the collection of independently committed table snapshots.
- Run production transformations in bounded mode and compare them with an independently implemented batch oracle using shared golden fixtures. Canonically select superseding trip/stop observations and define tie-breaking rules before comparing aggregates.
- Rebuild ClickHouse in a separate generation, compare keys, values, counts, and input digests, then offer a local command to switch the serving view. Keep the previous generation available for rollback.
- Automate poller crash points, TaskManager failures during checkpoints, Kafka transaction recovery, sink restarts, and partial multi-sink commits. Retain logs, input manifests, mismatch reports, and recovery timings.
- Add conservative Iceberg maintenance commands for compaction and snapshot expiration, with explicit protection for checkpoint, replay, and benchmark references. Keep raw-body cleanup disabled until reference tracking is tested.

Acceptance: the fault suite accounts for every acknowledged snapshot, reproduces the accepted aggregate set, reports all intentional rejections, and rebuilds ClickHouse without double counting. Failed parity blocks the serving switch.

Suggested commits: `feat: replay pinned lakehouse snapshots`, `test: compare batch and stream results`, `test: exercise checkpoint failures`, `feat: add guarded lake maintenance`.

### 8. Reproducible benchmarks

Deliverables:

- Add a seeded generator for steady, burst, disorder, duplicate, hot-agency, replay, and failure workloads. Preserve canonical input manifests and distinguish synthetic agencies from real feeds.
- Keep a small correctness workload for routine runs. Add explicit larger profiles, including a one-billion-event replay, only after measuring storage requirements and validating the generator at smaller scale.
- Record hardware, image digests, dependency versions, input size/distribution, JVM settings, state backend, parallelism, partitions, compression, checkpoint interval, watermark policy, warm-up, and measurement duration.
- Repeat each measured scenario at least three times and report median plus spread. Include throughput, latency quantiles, source freshness, watermark/Kafka lag, checkpoint duration, recovery time, CPU, memory, disk activity, and parity results.
- Bound the workload generator and visibility probes so their own saturation is detectable. Use synchronized clocks or a single measurement host for latency measurements.

Acceptance: `make benchmark-small` produces raw observations and a report that another developer can reproduce. Performance claims are gated on passing correctness and complete artifacts; the large throughput targets remain unclaimed until measured.

Suggested commits: `feat: generate benchmark workloads`, `feat: report benchmark evidence`.

## Implementation workflow

Keep code, focused tests, and necessary documentation together in small Conventional Commits. The suggested subjects above are boundaries, not a requirement to commit unverified intermediate work. Leave the README as a short description, current status, and a few working commands; put design detail and runbooks under `docs/`.

Use unit and schema tests on every relevant change, service integration tests when a storage or messaging boundary changes, and the full fixture smoke test once phase 5 lands. Add CI configuration locally for these checks; fault and large benchmark suites run separately from the normal test path. A phase is complete only when its acceptance checks pass, and any untested environment dependency is recorded explicitly.

The first useful ingestion milestone is phase 2: continuously fetch, store, and publish a fixture. The first queryable lakehouse milestone is phase 5. Recovery evidence and performance claims come after those paths work.

## Guardrails

- Live and replay paths share transformation code or golden fixtures.
- Schema compatibility is checked in CI before deployment.
- No benchmark claim appears in the README until its complete evidence is committed.
- One billion events is a benchmark profile, not a normal test prerequisite.
- Image tags are pinned for development; benchmark reports also capture immutable digests.
