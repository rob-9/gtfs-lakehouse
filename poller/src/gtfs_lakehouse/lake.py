"""Local REST-catalog access and resumable schedule publication."""

import json
import os

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo

from .identity import canonical_json, sha256_hex
from .schedules import parse_archive


def catalog():
    return load_catalog(
        "local",
        type="rest",
        uri=os.getenv("CATALOG_URI", "http://localhost:8181"),
        **{
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "lakehouse"),
            "s3.secret-access-key": os.getenv(
                "AWS_SECRET_ACCESS_KEY", "local-lakehouse-secret"
            ),
            "s3.region": "us-east-1",
            "s3.force-virtual-addressing": "false",
        },
    )


def table(name, rows):
    client = catalog()
    try:
        client.create_namespace("gtfs")
    except NamespaceAlreadyExistsError:
        pass
    try:
        return client.load_table(f"gtfs.{name}")
    except NoSuchTableError:
        return client.create_table(
            f"gtfs.{name}", schema=rows.schema, properties={"format-version": "2"}
        )


def load_schedule(body, agency, effective_from):
    from datetime import datetime

    instant = datetime.fromisoformat(effective_from)
    if instant.tzinfo is None:
        raise ValueError("effective_from requires a timezone")
    version = sha256_hex(body)
    schedule = parse_archive(body)
    publication_id = sha256_hex(canonical_json([agency, version]))
    sample = pa.Table.from_pylist(
        [
            {
                "publication_id": publication_id,
                "agency_id": agency,
                "schedule_version": version,
                "effective_from": instant.isoformat(),
                "manifest": "",
            }
        ]
    )
    versions = table("schedule_versions", sample)
    existing = (
        versions.scan(row_filter=EqualTo("publication_id", publication_id))
        .to_arrow()
        .to_pylist()
    )
    if existing:
        if existing[0]["effective_from"] != instant.isoformat():
            raise ValueError("existing archive has a different effective boundary")
        return json.loads(existing[0]["manifest"])
    previous = (
        versions.scan(row_filter=EqualTo("agency_id", agency)).to_arrow().to_pylist()
    )
    if any(
        datetime.fromisoformat(row["effective_from"]) >= instant for row in previous
    ):
        raise ValueError(
            "new schedule versions must advance the effective boundary; retroactive loads are unsupported"
        )
    manifest = {
        "agency_id": agency,
        "schedule_version": version,
        "effective_from": instant.isoformat(),
        "tables": {},
    }
    for name, rows in sorted(schedule.items()):
        if not rows:
            continue
        frame = pa.Table.from_pylist(
            [row | {"publication_id": publication_id} for row in rows]
        )
        target = table(name, frame)
        if (
            not target.scan(row_filter=EqualTo("publication_id", publication_id))
            .to_arrow()
            .num_rows
        ):
            target.append(frame)
        manifest["tables"][name] = target.current_snapshot().snapshot_id
    manifest["schedule"] = (
        schedule  # Fixture-sized broadcast index; local profile only.
    )
    record = sample.to_pylist()[0] | {"manifest": json.dumps(manifest, sort_keys=True)}
    versions.append(pa.Table.from_pylist([record]))
    return manifest


def publish_schedule(manifest):
    """Retryable publication: the committed Iceberg manifest is the durable outbox."""
    from confluent_kafka import Producer
    from .services import kafka_address

    producer = Producer(
        {
            "bootstrap.servers": kafka_address(),
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    errors = []
    body = canonical_json(manifest)
    if len(body) > 900000:
        raise ValueError("schedule exceeds local broadcast profile size")
    producer.produce(
        "gtfs.schedule.versions",
        key=canonical_json([manifest["agency_id"], manifest["schedule_version"]]),
        value=body,
        on_delivery=lambda error, message: errors.append(error) if error else None,
    )
    if producer.flush(30) or errors:
        raise RuntimeError(
            "schedule manifest publication failed; rerun the loader to retry"
        )
