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
        "metric_inputs",
    ):
        snapshot = client.load_table(f"gtfs.{name}").current_snapshot()
        if snapshot is None:
            raise ValueError(f"{name} has no committed snapshot")
        snapshots[name] = snapshot.snapshot_id
    manifest = {
        "source_generation": "live-v2",
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
    if generation in ("live-v1", "live-v2") or not generation:
        raise ValueError("rebuild requires a separate nonempty generation")
    # Only compare closed windows present in the pinned aggregate snapshot.
    source_generation = manifest.get("source_generation", "live-v1")
    live = [row for row in records(manifest, "route_window_metrics") if row["generation"] == source_generation]
    keys = {metric_key(row) for row in live}
    enriched = [
        row
        for row in records(manifest, "metric_inputs" if "metric_inputs" in manifest["snapshots"] else "enriched_events")
        if row.get("unmatched_reason") is None and metric_key(row) in keys
        and row.get("generation", source_generation) == source_generation
    ]
    expected = aggregate(enriched, generation=source_generation)
    unique = {metric_key(row): row for row in live}
    expected_by_key = {metric_key(row): row for row in expected}
    if expected_by_key != unique:
        differences = []
        for key in sorted(expected_by_key.keys() | unique.keys()):
            batch, stream = expected_by_key.get(key, {}), unique.get(key, {})
            fields = {field: {"batch": batch.get(field), "stream": stream.get(field)}
                      for field in batch.keys() | stream.keys() if batch.get(field) != stream.get(field)}
            if fields:
                differences.append({"key": key, "fields": fields})
        raise ValueError("stream/batch parity failed; generation not written: " + json.dumps(differences[:10], sort_keys=True))
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
