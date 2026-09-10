"""A retry-safe ClickHouse projection of the committed aggregate changelog."""

import json
import os
import signal
from pathlib import Path

import httpx
from confluent_kafka import Consumer

from .identity import canonical_json
from .services import kafka_address


def query(sql, **params):
    response = httpx.post(
        os.getenv("CLICKHOUSE_URL", "http://localhost:8123"),
        content=sql.encode(),
        params={f"param_{key}": str(value) for key, value in params.items()},
        auth=(
            os.getenv("CLICKHOUSE_USER", "lakehouse"),
            os.getenv("CLICKHOUSE_PASSWORD", "local-lakehouse-secret"),
        ),
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def bootstrap():
    path = Path(__file__).resolve().parents[3] / "clickhouse/ddl/serving.sql"
    for statement in path.read_text().split(";"):
        if statement.strip():
            query(statement)


def insert(records):
    rows = []
    for record in records:
        row = {
            key: record[key]
            for key in (
                "generation",
                "agency_id",
                "route_id",
                "direction_id",
                "service_date",
                "window_start",
                "version",
            )
        }
        row["record_json"] = canonical_json(record).decode()
        rows.append(json.dumps(row))
    if rows:
        query("INSERT INTO gtfs.route_metrics FORMAT JSONEachRow\n" + "\n".join(rows))


def latest(generation="live-v1"):
    result = query(
        "SELECT record_json FROM gtfs.route_metrics_latest WHERE generation = {generation:String} ORDER BY agency_id, route_id, direction_id, service_date, window_start FORMAT JSONEachRow",
        generation=generation,
    )
    return [json.loads(json.loads(line)["record_json"]) for line in result.splitlines()]


def serve(once=False):
    bootstrap()
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_address(),
            "group.id": "gtfs-clickhouse-v1",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe(["gtfs.route.metrics"])
    stopped = False

    def stop(*args):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopped:
            messages = consumer.consume(num_messages=100, timeout=10)
            if messages:
                if any(message.error() for message in messages):
                    raise RuntimeError("Kafka aggregate consumption failed")
                insert([json.loads(message.value()) for message in messages])
                # Insert happens before offsets. A crash in between safely redelivers rows.
                for message in messages:
                    consumer.store_offsets(message=message)
                consumer.commit(asynchronous=False)
                print(json.dumps({"inserted": len(messages)}), flush=True)
            if once:
                return
    finally:
        consumer.close()
