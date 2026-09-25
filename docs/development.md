# Local development

## Start and inspect

`make demo` starts the platform, loads a synthetic schedule, builds/submits the Flink job, starts managed fixture/poller/serving containers, and runs a fresh end-to-end correctness check. Initial downloads include the Java connectors and container images. Allow roughly 8 GiB of Docker memory and several GiB of disk space. The laptop profile has one Kafka broker and is not highly available.

`make status` lists containers. Flink is at http://localhost:8081, MinIO at http://localhost:9001, and ClickHouse at http://localhost:8123. Local credentials are `lakehouse` / `local-lakehouse-secret`; Grafana uses `admin` / `local-lakehouse-secret`. Host ports bind to localhost. These development credentials are not a deployment configuration.

`make down` saves the active job before stopping all profiles and preserves volumes. `make up` also saves and restores an active job when updating the platform. Restore-point tracking lives in `var/flink-state.json`; retain it with the volumes. After an unclean shutdown, submission looks for the tracked job's latest completed checkpoint. No cleanup target deletes lakehouse data. `make submit` avoids creating a second active pipeline; `make redeploy` restores the new build from a savepoint. Fresh submission over retained history without a tracked restore point is rejected to prevent duplicate replay.

## Versions and wire format

The tested core is Kafka 3.9.1, Flink 1.20.5 on Java 17, Iceberg 1.10.1, and ClickHouse 26.3.29.7. Flink's Kafka connector is 3.4.0-1.20. Jackson dependencies use one BOM to avoid runtime linkage conflicts. The pairing follows Iceberg's [engine compatibility guidance](https://iceberg.apache.org/multi-engine-support/). Python dependencies are locked in `poller/uv.lock`; dbt uses its own lockfile.

Normalized Kafka records are single-record Avro object containers carrying their writer schema. This favors inspectability over compact framing; its per-record overhead must be included in performance measurements. Raw manifests, schedule manifests, rejection records, and metric changelogs use versioned JSON. Iceberg history currently has typed identity/event-time columns plus the complete record JSON, rather than a fully expanded analytical schema.

## Polling and schedules

For host development, run `make fixtures` and then `make poll-fixtures` in separate terminals. Managed containers use `config/container-feeds.toml`; host polling uses `config/feeds.toml`. The local profile permits up to eight feeds, each with one in-flight request. Optional `authorization_env` names an environment variable containing the Authorization header. Polling does not follow redirects or retain query-string credentials in stored URLs.

The fixture response is stable and subsequent polls return 304. The smoke test publishes a later fixture explicitly to advance event time; it does not wait seven wall-clock minutes for a final window. Synthetic timestamps are unsuitable for source-freshness or live-latency claims.

Load another static schedule with:

```sh
uv run --project poller --locked python -m gtfs_lakehouse load-schedule schedule.zip --agency my-agency --effective-from 2026-09-10T00:00:00Z
```

The archive must fit the local size limits and contain one agency. The initial join requires explicit trip start dates, supports calendar exceptions and times beyond midnight, and rejects frequency-based schedules. Version boundaries must advance chronologically; retroactive replacement requires a separate replay policy. Schedule broadcasts embed a small lookup index and are capped at 900 kB. Schema evolution, larger indexes, and multiple loader writers need further work.

## Queries, quality, and dashboards

`make query` returns final windows. `make quality` builds the dbt view and runs uniqueness/range checks. `make observability` starts the exporter, Prometheus, and Grafana; open http://localhost:3000 for the provisioned dashboard. The exporter covers running jobs, checkpoint counts/duration, serving windows, and prediction samples. The managed poller exposes per-feed HTTP attempts, successful responses (200/304), errors by stage, acknowledged publications, outbox backlog and age, and source freshness. Run `make workers` to start or update the poller; Prometheus scrapes its internal port 9109. Detailed partition lag and backpressure remain follow-up instrumentation.

For host polling, add `--metrics-port 9109` to the `poll` command to expose `/metrics` on localhost. `--metrics-host` controls the bind address. Counters reset on process restart. Outbox backlog and its receipt timestamp are restored from SQLite before publisher initialization; each feed can hold at most one pending snapshot. Other timestamps are unknown (`NaN`) until observed in the current process. Source freshness uses the newest normalized event in the last acknowledged snapshot, and remains unknown if that snapshot has no valid events. A 304 refreshes HTTP health without refreshing source freshness. Publication counts are acknowledged attempts, not unique snapshot counts, because recovery may republish a committed snapshot. Labels contain configured agency/feed IDs and bounded status/stage values, never endpoint URLs, credentials, or exception text.

If a feed fails, check the poller reachability panel, time since HTTP success, and error stage, then inspect `docker compose logs poller` and its outbox volume before changing validators. Increasing source age with successful 304 responses means the endpoint is reachable but its data is unchanged. Synthetic fixtures deliberately have old observation times. A pending snapshot with increasing age and publish/initialize errors points to object storage or Kafka; pending work blocks the next fetch until publication succeeds. Unknown source age after a restart is expected until another snapshot is published. For a stalled pipeline, inspect the Flink job exception and checkpoint views. For sink lag, inspect `docker compose logs serving`; offsets advance only after inserts succeed. Repeated rows are expected after a sink retry, so query `gtfs.route_metrics_latest`, not physical staging rows.

## Replay and failures

```sh
uv run --project poller --locked python -m gtfs_lakehouse pin var/replay/check.json
uv run --project poller --locked python -m gtfs_lakehouse replay var/replay/check.json --generation replay-check
uv run --project poller --locked python -m gtfs_lakehouse query --generation replay-check
make test-recovery
```

Pinning creates a new manifest without overwriting an existing one. Rebuild compares `live-v2` final windows with an independent aggregation of the pinned `metric_inputs` admission ledger. A mismatch prevents writes to the new serving generation and reports the differing fields. Generations remain explicitly selectable by query; there is no automatic production cutover. A snapshot collection is not an atomic transaction across Iceberg tables, so pin during a quiet period and retry with a fresh manifest if parity fails. Legacy `live-v1` development data is retained separately because it predates the persisted admission cutoff and ledger.

For an upgrade leftover with completely missing admission records, `migrate-inputs MANIFEST` previews a backfill. It requires the complete enriched input set to reproduce both the stored digest and every aggregate value. `--apply` appends explicitly marked migration records and refuses a changed ledger snapshot. Partial inputs, conflicting aggregates, and mismatched digests require investigation. A fresh checkout does not need this migration.

The recovery test briefly pauses this project's TaskManager to hold an explicitly triggered checkpoint open, kills/restarts it, and checks restored state plus final aggregate parity. It then restarts the worker again and verifies that unseen old events are audited instead of reopening a closed window. Reports and TaskManager logs remain under `var/`. Additional broker, sink, and poller crash matrices remain in the implementation plan. Automatic snapshot expiration and orphan deletion are intentionally not enabled while checkpoint/replay reference tracking is incomplete.

## Benchmark evidence

`make benchmark-small` performs one unmeasured warm-up and three measured fixture runs. Reports under `benchmark/artifacts/` include commands, environment, logs, completion times, and parity. This measures end-to-end smoke completion, not sustained events/s. The large replay and throughput targets require separate workload generation, hardware characterization, and load testing.

## Scheduled-service coverage reports

Pin schedules and enriched history, then select the schedule version returned by `load-schedule` or `load-fixture-schedule`:

```sh
uv run --project poller --locked python -m gtfs_lakehouse pin-coverage var/coverage/inputs.json
uv run --project poller --locked python -m gtfs_lakehouse coverage var/coverage/inputs.json --agency demo --schedule-version VERSION --service-date 2026-09-10 --as-of 2026-09-10T16:00:00Z --grace-seconds 300 --output var/coverage/report.json
```

Both pinning and report output refuse to overwrite an existing file. A regular replay manifest also works if it contains schedule and enriched-event snapshots. An absent or empty enriched table is pinned explicitly as empty, so zero telemetry still produces a schedule-derived denominator and never falls back to reading current data. Coverage distinguishes trips not yet due, explicit reported cancellations, telemetry present, and missing telemetry. See `docs/metrics.md` for the denominator and limits; this command reads Iceberg and writes only a local JSON report.
