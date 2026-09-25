"""Bounded checks of local service behavior, not just process liveness."""

import time
import uuid

import httpx
from confluent_kafka import Consumer

from .fixtures import realtime
from .identity import snapshot_id, sha256_hex, canonical_json
from .ingestion import Publisher
from .models import RawSnapshot
from .services import bootstrap, kafka_address, s3
from .wire import decode_event


def platform():
    bootstrap()
    response = httpx.get("http://localhost:8181/v1/config", timeout=20)
    response.raise_for_status()
    response = httpx.post(
        "http://localhost:8123/",
        content="SELECT 1",
        auth=("lakehouse", "local-lakehouse-secret"),
        timeout=20,
    )
    response.raise_for_status()
    assert response.text.strip() == "1"
    token = uuid.uuid4().hex
    body = realtime()
    snapshot = RawSnapshot(
        snapshot_id(feed_id=token, body=body),
        "demo",
        token,
        time.time_ns() // 1_000_000,
        "fixture://local",
        200,
        None,
        None,
        None,
        body,
    )
    Publisher(token).publish(snapshot)
    source = sha256_hex(canonical_json([snapshot.agency_id, snapshot.feed_id]))
    restored = (
        s3()
        .get_object(Bucket="raw", Key=f"{source}/{snapshot.snapshot_id}.pb")["Body"]
        .read()
    )
    assert restored == body
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_address(),
            "group.id": f"smoke-{token}",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["gtfs.normalized.events"])
    found = set()
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline and len(found) < 4:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            event = decode_event(message.value())
            if event["feed_id"] == token:
                found.add(event["event_id"])
        assert len(found) == 4, (
            f"expected 4 normalized fixture events, found {len(found)}"
        )
    finally:
        consumer.close()
    print(
        "platform smoke passed: catalog, ClickHouse, transactional Kafka and fixture normalization"
    )


def pipeline(fault=None):
    import json
    from .fixtures import static_zip
    from .lake import catalog, load_schedule, publish_schedule
    from .serving import serve, latest
    from .oracle import aggregate
    from pyiceberg.exceptions import NoSuchTableError

    bootstrap()
    token = "smoke-" + uuid.uuid4().hex[:12]
    client = catalog()
    try:
        existing = (
            client.load_table("gtfs.normalized_events").scan().to_arrow().to_pylist()
        )
    except NoSuchTableError:
        raise RuntimeError(
            "submit the Flink job before running the pipeline smoke test"
        )
    base = max([int(time.time())] + [row["observed_at"] // 1000 for row in existing])
    base = (base // 300 + 1) * 300
    publish_schedule(load_schedule(static_zip(), token, "2020-01-01T00:00:00+00:00"))
    publisher = Publisher(token)

    def publish(timestamp):
        body = realtime(timestamp)
        snapshot = RawSnapshot(
            snapshot_id(feed_id=token, body=body),
            token,
            token,
            time.time_ns() // 1_000_000,
            "fixture://local",
            200,
            None,
            None,
            None,
            body,
        )
        publisher.publish(snapshot)

    publish(base)
    publish(base)  # Transactional duplicate: Flink must deduplicate it.
    if fault is not None:
        fault()
    deadline = time.monotonic() + 180
    enriched = []
    while time.monotonic() < deadline:
        target = client.load_table("gtfs.enriched_events")
        enriched = [
            json.loads(row["record_json"])
            for row in target.scan().to_arrow().to_pylist()
        ]
        enriched = [row for row in enriched if row["agency_id"] == token]
        if len(enriched) == 4:
            break
        time.sleep(2)
    assert len(enriched) == 4, (
        f"expected four committed enriched records, found {len(enriched)}"
    )
    assert all(row.get("unmatched_reason") is None for row in enriched), enriched
    publish(base + 900)  # Explicit fixture watermark advance, not a wall-clock sleep.
    expected = aggregate(enriched)
    expected_v3 = aggregate(enriched, generation="live-v3")
    while time.monotonic() < deadline:
        serve(once=True)
        observed = [
            row
            for row in latest()
            if row["agency_id"] == token and row["window_start"] == base * 1000
        ]
        observed_v3 = [
            row
            for row in latest("live-v3")
            if row["agency_id"] == token and row["window_start"] == base * 1000
        ]
        if observed and observed_v3:
            assert observed_v3 == expected_v3, {
                "observed": observed_v3,
                "expected": expected_v3,
            }
            assert observed == expected, {"observed": observed, "expected": expected}
            print(
                json.dumps(
                    {
                        "pipeline_smoke": "passed",
                        "agency": token,
                        "events": 4,
                        "metric": observed[0],
                        "metric_v3": observed_v3[0],
                    }
                )
            )
            return {
                "agency": token,
                "events": 4,
                "metric": observed[0],
                "metric_v3": observed_v3[0],
            }
        time.sleep(2)
    raise AssertionError(
        "final aggregate did not become queryable before smoke timeout"
    )
