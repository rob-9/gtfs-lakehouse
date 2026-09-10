"""Kafka v1: Avro object containers for events, versioned JSON for manifests/audits."""

import json
from io import BytesIO
from pathlib import Path

import fastavro

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[3] / "schemas/gtfs_event.avsc").read_text()
)


def encode_event(record):
    buffer = BytesIO()
    fastavro.writer(buffer, SCHEMA, [record])
    return buffer.getvalue()


def decode_event(body):
    return next(fastavro.reader(BytesIO(body)))
