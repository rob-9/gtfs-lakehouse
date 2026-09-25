"""Schedule-driven telemetry coverage from explicitly pinned Iceberg inputs."""

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from .identity import canonical_json
from .schedules import active, service_instant


def instant(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("as-of time requires a timezone")
    return int(parsed.timestamp() * 1000)


def coverage(schedule_manifest, events, service_date, as_of, grace_seconds=300):
    """Classify due scheduled trips; telemetry presence does not prove operation."""
    if (
        not isinstance(grace_seconds, int)
        or isinstance(grace_seconds, bool)
        or not 0 <= grace_seconds <= 86400
    ):
        raise ValueError("grace seconds must be an integer between 0 and 86400")
    day = date.fromisoformat(service_date)
    cutoff = instant(as_of)
    schedule = schedule_manifest["schedule"]
    agency, version = (
        schedule_manifest["agency_id"],
        schedule_manifest["schedule_version"],
    )
    zone = schedule["agency"][0]["agency_timezone"]
    starts = defaultdict(list)
    for stop in schedule["stop_times"]:
        starts[stop["trip_id"]].append(stop)
    trips = {
        trip["trip_id"]: trip
        for trip in schedule["trips"]
        if active(schedule, trip["service_id"], day)
    }
    evidence = defaultdict(dict)
    excluded = defaultdict(int)
    unique = {}
    for event in events:
        if event.get("agency_id") != agency:
            continue
        previous = unique.get(event["event_id"])
        if previous is not None and previous != event:
            raise ValueError("conflicting records for one event ID")
        unique[event["event_id"]] = event
    for event in unique.values():
        if (
            event.get("agency_id") != agency
            or event.get("service_date") != service_date
        ):
            continue
        if event.get("unmatched_reason") is not None:
            excluded["unmatched"] += 1
            continue
        if event.get("schedule_version") != version:
            excluded["schedule_version"] += 1
            continue
        if event["observed_at"] > cutoff:
            excluded["after_as_of"] += 1
            continue
        if event.get("trip_id") not in trips:
            excluded["unscheduled_trip"] += 1
            continue
        evidence[event["trip_id"]][event["event_id"]] = event
    details = []
    summaries = {}
    for trip_id, trip in sorted(trips.items()):
        stops = starts[trip_id]
        if not stops:
            raise ValueError(f"scheduled trip has no stop times: {trip_id}")
        ordered_stops = sorted(stops, key=lambda stop: int(stop["stop_sequence"]))
        departure = (
            service_instant(day, ordered_stops[0]["departure_time"], zone) * 1000
        )
        completion = (
            max(service_instant(day, stop["departure_time"], zone) for stop in stops)
            * 1000
        )
        due_at = completion + grace_seconds * 1000
        ordered = sorted(
            evidence[trip_id].values(),
            key=lambda event: (event["observed_at"], event["event_id"]),
        )
        canceled = False
        telemetry = False
        for event in ordered:
            payload = event["payload"]
            relationship = (payload.get("trip") or {}).get("schedule_relationship")
            if relationship == "CANCELED":
                canceled = True
            elif payload.get("trip_update") is not None:
                canceled = False
                telemetry = True
            elif payload.get("vehicle_position") is not None:
                telemetry = True
        due = cutoff >= due_at
        status = (
            "not_due"
            if not due
            else "canceled"
            if canceled
            else "telemetry_present"
            if telemetry
            else "missing_telemetry"
        )
        direction = int(trip.get("direction_id") or -1)
        key = (trip["route_id"], direction)
        summary = summaries.setdefault(
            key,
            dict(
                route_id=key[0],
                direction_id=key[1],
                scheduled_trip_count=0,
                due_trip_count=0,
                not_due_trip_count=0,
                telemetry_trip_count=0,
                canceled_trip_count=0,
                missing_telemetry_trip_count=0,
            ),
        )
        summary["scheduled_trip_count"] += 1
        summary["due_trip_count"] += int(due)
        counter = {
            "not_due": "not_due_trip_count",
            "canceled": "canceled_trip_count",
            "telemetry_present": "telemetry_trip_count",
            "missing_telemetry": "missing_telemetry_trip_count",
        }[status]
        summary[counter] += 1
        details.append(
            dict(
                trip_id=trip_id,
                route_id=trip["route_id"],
                direction_id=direction,
                scheduled_departure=departure,
                scheduled_completion=completion,
                due_at=due_at,
                status=status,
                reported_canceled=canceled,
                evidence_count=len(ordered),
                input_digest=hashlib.sha256(
                    "\n".join(sorted(evidence[trip_id])).encode()
                ).hexdigest(),
            )
        )
    routes = []
    for key in sorted(summaries):
        summary = summaries[key]
        eligible = summary["due_trip_count"] - summary["canceled_trip_count"]
        summary["telemetry_coverage_rate"] = (
            summary["telemetry_trip_count"] / eligible if eligible else None
        )
        routes.append(summary)
    return dict(
        schema_version=1,
        agency_id=agency,
        schedule_version=version,
        service_date=service_date,
        as_of=cutoff,
        grace_seconds=grace_seconds,
        agency_timezone=zone,
        schedule_digest=hashlib.sha256(canonical_json(schedule_manifest)).hexdigest(),
        excluded_event_counts=dict(sorted(excluded.items())),
        routes=routes,
        trips=details,
    )


def pin_coverage(path):
    from .lake import catalog
    from pyiceberg.exceptions import NoSuchTableError

    client = catalog()
    snapshots = {}
    for name in ("schedule_versions", "enriched_events"):
        try:
            snapshot = client.load_table(f"gtfs.{name}").current_snapshot()
        except NoSuchTableError:
            if name != "enriched_events":
                raise
            snapshot = None
        if name == "schedule_versions" and snapshot is None:
            raise ValueError("no committed schedule versions")
        snapshots[name] = snapshot.snapshot_id if snapshot is not None else None
    manifest = dict(
        schema_version=1,
        created_at=datetime.now(timezone.utc).isoformat(),
        snapshots=snapshots,
    )
    write_report(path, manifest)
    return manifest


def report(manifest, agency, version, service_date, as_of, grace_seconds=300):
    from .lake import catalog

    if instant(as_of) > instant(manifest["created_at"]):
        raise ValueError("as-of time is after snapshot collection; pin a newer report")

    def rows(name):
        snapshot = manifest["snapshots"][name]
        # None pins an empty table, never the latest snapshot at read time.
        if snapshot is None:
            return []
        return (
            catalog()
            .load_table(f"gtfs.{name}")
            .scan(snapshot_id=snapshot)
            .to_arrow()
            .to_pylist()
        )

    schedules = [
        json.loads(row["manifest"])
        for row in rows("schedule_versions")
        if row["agency_id"] == agency and row["schedule_version"] == version
    ]
    if not schedules:
        raise ValueError("schedule version is absent from the pinned snapshot")
    if any(item != schedules[0] for item in schedules):
        raise ValueError("conflicting committed schedule manifests")
    result = coverage(
        schedules[0],
        [json.loads(row["record_json"]) for row in rows("enriched_events")],
        service_date,
        as_of,
        grace_seconds,
    )
    return result | {
        "snapshots": manifest["snapshots"],
        "snapshot_collection_created_at": manifest.get("created_at"),
    }


def write_report(path, result):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
