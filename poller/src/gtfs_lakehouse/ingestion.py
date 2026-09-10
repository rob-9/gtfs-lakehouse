"""A durable per-feed outbox with at-least-once transactional publication."""

import asyncio
import json
import os
import random
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path

import httpx
from confluent_kafka import Producer

from .identity import canonical_json, sha256_hex
from .models import RawSnapshot
from .normalize import normalize_feed
from .polling import FeedConfig, PollState, fetch_feed
from .services import kafka_address, s3
from .wire import encode_event


class Outbox:
    """One database per configured source, owned by one poller process."""

    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS pending (id INTEGER PRIMARY KEY CHECK(id=1), metadata TEXT, body BLOB)")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), etag TEXT, modified TEXT)")

    def state(self):
        row = self.db.execute("SELECT etag, modified FROM state WHERE id=1").fetchone()
        return PollState(*row) if row else PollState()

    def stage(self, snapshot):
        metadata = asdict(snapshot)
        body = metadata.pop("body")
        with self.db:
            self.db.execute("INSERT INTO pending VALUES (1, ?, ?)", (json.dumps(metadata), body))

    def pending(self):
        row = self.db.execute("SELECT metadata, body FROM pending WHERE id=1").fetchone()
        return RawSnapshot(**json.loads(row[0]), body=row[1]) if row else None

    def acknowledge(self, state):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES (1, ?, ?)", (state.etag, state.last_modified))
            self.db.execute("DELETE FROM pending WHERE id=1")


class Publisher:
    def __init__(self, identity):
        self.objects = s3()
        self.producer = Producer({"bootstrap.servers": kafka_address(),
            "transactional.id": f"gtfs-poller-{identity}", "enable.idempotence": True,
            "acks": "all", "compression.type": "zstd", "transaction.timeout.ms": 60000,
            "message.max.bytes": 20 * 1024 * 1024})
        self.producer.init_transactions(30)

    def publish(self, snapshot):
        source = sha256_hex(canonical_json([snapshot.agency_id, snapshot.feed_id]))
        key = f"{source}/{snapshot.snapshot_id}.pb"
        self.objects.put_object(Bucket="raw", Key=key, Body=snapshot.body,
                                ContentType="application/x-protobuf")
        manifest = asdict(snapshot)
        manifest.pop("body")
        manifest.update(schema_version=1, object_uri=f"s3://raw/{key}",
                        body_sha256=sha256_hex(snapshot.body), body_bytes=len(snapshot.body))
        events, failures = normalize_feed(snapshot.body, agency_id=snapshot.agency_id,
                                         feed_id=snapshot.feed_id, ingested_at=snapshot.fetched_at)
        errors = []

        def delivered(error, message):
            if error:
                errors.append(error)

        self.producer.begin_transaction()
        try:
            self.producer.produce("gtfs.raw.snapshots", key=source, value=canonical_json(manifest), on_delivery=delivered)
            for event in events:
                event_key = canonical_json([event.agency_id, event.feed_id, event.entity_type, event.entity_id])
                self.producer.produce("gtfs.normalized.events", key=event_key,
                                      value=encode_event(event.to_dict()), on_delivery=delivered)
            for failure in failures:
                record = failure.to_dict() | {"schema_version": 1, "object_uri": manifest["object_uri"]}
                self.producer.produce("gtfs.dead_letter", key=source, value=canonical_json(record), on_delivery=delivered)
            if self.producer.flush(30) or errors:
                raise RuntimeError("Kafka delivery failed")
            self.producer.commit_transaction(30)
        except Exception:
            self.producer.abort_transaction(30)
            raise
        return {"snapshot_id": snapshot.snapshot_id, "events": len(events), "failures": len(failures)}


async def poll_source(entry, state_dir, stop, once=False):
    source_id = sha256_hex(canonical_json([entry["agency_id"], entry["feed_id"], entry["url"]]))
    box = Outbox(Path(state_dir) / f"{source_id}.sqlite")
    publisher = None
    interval = float(entry.get("interval_seconds", 30))
    if interval <= 0:
        raise ValueError("poll interval must be positive")
    config = FeedConfig(**{key: entry[key] for key in
                          ("agency_id", "feed_id", "url", "timeout_seconds", "max_response_bytes") if key in entry})
    headers = {}
    if entry.get("authorization_env"):
        headers["Authorization"] = os.environ[entry["authorization_env"]]
    failures = 0
    try:
        async with httpx.AsyncClient(headers=headers) as client:
            while not stop.is_set():
                delay = interval
                try:
                    if publisher is None:
                        publisher = await asyncio.to_thread(Publisher, source_id)
                    pending = box.pending()
                    if pending is None:
                        result = await fetch_feed(client, config, box.state())
                        pending = result.snapshot
                        if pending is not None:
                            box.stage(pending)
                        else:
                            box.acknowledge(result.state)
                    if pending is not None:
                        report = await asyncio.to_thread(publisher.publish, pending)
                        box.acknowledge(PollState(pending.etag, pending.last_modified))
                        print(json.dumps(report), flush=True)
                    failures = 0
                except Exception as exc:
                    if once:
                        raise
                    failures += 1
                    delay = min(300, interval * 2 ** min(failures, 8)) * random.uniform(0.8, 1.2)
                    if isinstance(exc, httpx.HTTPStatusError):
                        retry = exc.response.headers.get("retry-after", "")
                        if retry.isdigit():
                            delay = max(delay, min(float(retry), 3600))
                    # Exceptions can contain credential-bearing URLs; log only type and configured identity.
                    print(json.dumps({"feed": entry["feed_id"], "error": type(exc).__name__}), flush=True)
                    publisher = None
                if once:
                    return
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
    finally:
        box.db.close()
