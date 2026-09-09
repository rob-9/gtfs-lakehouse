# GTFS Streaming Lakehouse

This project is building a replayable transit-data pipeline around GTFS Realtime, Kafka, Flink, Iceberg, and ClickHouse. The first slice handles conditional feed polling, protobuf normalization, stable event IDs, and structured dead letters.

The consistency boundary is deliberate: HTTP polling is at least once, events are deduplicated by content-derived IDs, Iceberg is the replay source of truth, and ClickHouse is a rebuildable serving layer.

## Try it

Install [uv](https://docs.astral.sh/uv/), then run:

```sh
make test
```

The test suite builds GTFS-RT fixtures in memory and checks deterministic identity, timestamp fallback, validation, and conditional HTTP requests.

The schemas live in `schemas/`, poller code in `poller/`, and the staged roadmap in [`docs/implementation-plan.md`](docs/implementation-plan.md). The local streaming stack and schedule enrichment are the next milestones; benchmark numbers will only be published with reproducible evidence.
