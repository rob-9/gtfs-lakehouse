# GTFS Streaming Lakehouse

This project is building a replayable transit-data pipeline around GTFS Realtime, Kafka, Flink, Iceberg, and ClickHouse. The first slice handles conditional feed polling, protobuf normalization, stable event IDs, and structured dead letters.

The planned pipeline uses at-least-once polling, content-derived IDs for deduplication, Iceberg for replay, and ClickHouse as a rebuildable serving layer. Storage, publication, and streaming deduplication are still to come.

## Try it

Install [uv](https://docs.astral.sh/uv/), then run:

```sh
make test
```

The tests cover correction identity, timestamp fallback, malformed feeds, conditional HTTP requests, and Avro serialization.

The schemas live in `schemas/`, poller code in `poller/`, and the staged roadmap in [`docs/implementation-plan.md`](docs/implementation-plan.md). The local streaming stack and schedule enrichment are the next milestones; benchmark numbers will only be published with reproducible evidence.
