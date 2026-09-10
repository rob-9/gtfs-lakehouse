"""Pin committed Iceberg inputs and verify an isolated serving rebuild."""

import json
from datetime import datetime, timezone
from pathlib import Path

from .lake import catalog
from .oracle import aggregate
from .serving import bootstrap, insert, latest


def pin(path):
    client = catalog()
    snapshots = {}
    for name in (
        "normalized_events",
        "enriched_events",
        "route_window_metrics",
        "schedule_versions",
    ):
        snapshot = client.load_table(f"gtfs.{name}").current_snapshot()
        if snapshot is None:
            raise ValueError(f"{name} has no committed snapshot")
        snapshots[name] = snapshot.snapshot_id
    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "snapshots": snapshots,
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "x") as output:
        json.dump(manifest, output, indent=2)
    return manifest


def records(manifest, name):
    target = catalog().load_table(f"gtfs.{name}")
    return [
        json.loads(row["record_json"])
        for row in target.scan(snapshot_id=manifest["snapshots"][name])
        .to_arrow()
        .to_pylist()
    ]


def rebuild(manifest, generation):
    if generation == "live-v1" or not generation:
        raise ValueError("rebuild requires a separate nonempty generation")
    # Only compare closed windows present in the pinned aggregate snapshot.
    live = records(manifest, "route_window_metrics")
    keys = {metric_key(row) for row in live}
    enriched = [
        row
        for row in records(manifest, "enriched_events")
        if row.get("unmatched_reason") is None and metric_key(row) in keys
    ]
    expected = aggregate(enriched)
    unique = {metric_key(row): row for row in live}
    if {metric_key(row): row for row in expected} != unique:
        raise ValueError(
            "stream/batch parity failed; serving generation was not written"
        )
    bootstrap()
    if latest(generation):
        raise ValueError("generation already exists; choose a fresh generation")
    rebuilt = [row | {"generation": generation} for row in expected]
    insert(rebuilt)
    visible = latest(generation)
    if {metric_key(row): row for row in rebuilt} != {
        metric_key(row): row for row in visible
    }:
        raise ValueError("ClickHouse rebuild parity failed")
    return {
        "parity": True,
        "windows": len(rebuilt),
        "generation": generation,
        "snapshots": manifest["snapshots"],
    }


def metric_key(row):
    timestamp = row.get("window_start", row.get("observed_at", 0))
    return (
        row["agency_id"],
        row["route_id"],
        row["direction_id"],
        row["service_date"],
        timestamp // 300000 * 300000,
    )
