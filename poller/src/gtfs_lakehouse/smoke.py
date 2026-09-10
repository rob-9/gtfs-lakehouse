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
    response = httpx.post("http://localhost:8123/", content="SELECT 1",
                          auth=("lakehouse", "local-lakehouse-secret"), timeout=20)
    response.raise_for_status()
    assert response.text.strip() == "1"
    token = uuid.uuid4().hex
    body = realtime()
    snapshot = RawSnapshot(snapshot_id(feed_id=token, body=body), "demo", token,
                           time.time_ns() // 1_000_000, "fixture://local", 200, None, None, None, body)
    Publisher(token).publish(snapshot)
    source = sha256_hex(canonical_json([snapshot.agency_id, snapshot.feed_id]))
    restored = s3().get_object(Bucket="raw", Key=f"{source}/{snapshot.snapshot_id}.pb")["Body"].read()
    assert restored == body
    consumer = Consumer({"bootstrap.servers": kafka_address(), "group.id": f"smoke-{token}",
                         "auto.offset.reset": "earliest", "isolation.level": "read_committed",
                         "enable.auto.commit": False})
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
        assert len(found) == 4, f"expected 4 normalized fixture events, found {len(found)}"
    finally:
        consumer.close()
    print("platform smoke passed: catalog, ClickHouse, transactional Kafka and fixture normalization")
