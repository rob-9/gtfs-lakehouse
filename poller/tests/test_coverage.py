from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from gtfs_lakehouse.coverage import coverage, pin_coverage, report, write_report
from gtfs_lakehouse.fixtures import realtime, static_zip
from gtfs_lakehouse.normalize import normalize_feed
from gtfs_lakehouse.schedules import enrich, parse_archive, service_instant


def inputs():
    schedule = parse_archive(static_zip())
    manifest = dict(
        agency_id="demo",
        schedule_version="v",
        effective_from="2020-01-01T00:00:00Z",
        schedule=schedule,
    )
    events, _ = normalize_feed(realtime(), agency_id="demo", feed_id="f", ingested_at=1)
    return manifest, [enrich(event.to_dict(), schedule, "v") for event in events]


def test_denominator_includes_scheduled_trips_with_no_telemetry():
    manifest, _ = inputs()
    result = coverage(manifest, [], "2026-09-10", "2026-09-10T16:00:00Z")
    (row,) = result["routes"]
    assert (
        row["scheduled_trip_count"]
        == row["due_trip_count"]
        == row["missing_telemetry_trip_count"]
        == 3
    )
    assert row["canceled_trip_count"] == 0
    assert row["telemetry_coverage_rate"] == 0


def test_cancellation_is_distinct_from_missing_and_predictions_only_prove_telemetry():
    manifest, events = inputs()
    result = coverage(manifest, events[1:], "2026-09-10", "2026-09-10T16:00:00Z")
    (row,) = result["routes"]
    # T1 has only a VehiclePosition, T2 a prediction, T3 an explicit cancellation.
    assert row["telemetry_trip_count"] == 2
    assert row["canceled_trip_count"] == 1
    assert row["missing_telemetry_trip_count"] == 0
    assert row["telemetry_coverage_rate"] == 1
    assert (
        coverage(manifest, events[1:] * 2, "2026-09-10", "2026-09-10T16:00:00Z")
        == result
    )
    assert (
        coverage(
            manifest, list(reversed(events[1:])), "2026-09-10", "2026-09-10T16:00:00Z"
        )
        == result
    )
    missing = coverage(manifest, events[1:3], "2026-09-10", "2026-09-10T16:00:00Z")[
        "routes"
    ][0]
    assert missing["missing_telemetry_trip_count"] == 1
    assert missing["telemetry_coverage_rate"] == 0.5


def test_grace_period_and_as_of_filter():
    manifest, events = inputs()
    before = coverage(manifest, events, "2026-09-10", "2026-09-10T15:14:59Z")
    assert before["routes"][0]["due_trip_count"] == 0
    assert before["routes"][0]["telemetry_coverage_rate"] is None
    after = coverage(manifest, events, "2026-09-10", "2026-09-10T15:15:00Z")
    assert after["routes"][0]["due_trip_count"] == 1
    assert after["routes"][0]["not_due_trip_count"] == 2
    earlier = coverage(manifest, events, "2026-09-10", "2026-09-10T14:59:00Z")
    assert earlier["excluded_event_counts"] == {"after_as_of": 4}


def test_vehicle_does_not_clear_cancellation_but_later_trip_update_does():
    manifest, events = inputs()
    vehicle = deepcopy(events[3])
    vehicle.update(
        trip_id="T3",
        event_id="later-vehicle",
        observed_at=events[2]["observed_at"] + 1000,
    )
    result = coverage(
        manifest, events + [vehicle], "2026-09-10", "2026-09-10T16:00:00Z"
    )
    assert result["trips"][2]["status"] == "canceled"
    update = deepcopy(events[0])
    update.update(
        trip_id="T3", event_id="later-update", observed_at=vehicle["observed_at"] + 1000
    )
    result = coverage(
        manifest, events + [vehicle, update], "2026-09-10", "2026-09-10T16:00:00Z"
    )
    assert result["trips"][2]["status"] == "telemetry_present"


@pytest.mark.parametrize("day", [date(2026, 3, 8), date(2026, 11, 1)])
def test_overnight_completion_uses_gtfs_clock_on_dst_days(day):
    manifest, _ = inputs()
    for stop in manifest["schedule"]["stop_times"]:
        stop["arrival_time"] = stop["departure_time"] = "25:00:00"
    due = service_instant(day, "25:00:00", "America/Los_Angeles") + 300
    before = datetime.fromtimestamp(due - 1, timezone.utc).isoformat()
    after = datetime.fromtimestamp(due, timezone.utc).isoformat()
    assert (
        coverage(manifest, [], day.isoformat(), before)["routes"][0]["due_trip_count"]
        == 0
    )
    assert (
        coverage(manifest, [], day.isoformat(), after)["routes"][0]["due_trip_count"]
        == 3
    )


def test_calendar_removal_and_version_mismatch_are_visible():
    manifest, events = inputs()
    result = coverage(
        manifest | {"schedule_version": "other"},
        events,
        "2026-09-10",
        "2026-09-10T16:00:00Z",
    )
    assert result["excluded_event_counts"] == {"schedule_version": 4}
    assert result["routes"][0]["missing_telemetry_trip_count"] == 3
    manifest["schedule"]["calendar_dates"] = [
        {"service_id": "daily", "date": "20260910", "exception_type": "2"}
    ]
    assert coverage(manifest, [], "2026-09-10", "2026-09-10T16:00:00Z")["routes"] == []


def test_invalid_inputs_and_conflicting_evidence_are_rejected():
    manifest, events = inputs()
    with pytest.raises(ValueError, match="timezone"):
        coverage(manifest, [], "2026-09-10", "2026-09-10T16:00:00")
    with pytest.raises(ValueError, match="grace"):
        coverage(manifest, [], "2026-09-10", "2026-09-10T16:00:00Z", -1)
    with pytest.raises(ValueError, match="conflicting"):
        coverage(
            manifest,
            events + [events[0] | {"vehicle_id": "other"}],
            "2026-09-10",
            "2026-09-10T16:00:00Z",
        )


def test_empty_snapshot_is_pinned_and_never_read_as_latest(tmp_path, monkeypatch):
    import json
    from gtfs_lakehouse import lake

    schedule, _ = inputs()
    schedules = Mock()
    schedules.current_snapshot.return_value = SimpleNamespace(snapshot_id=123)
    schedules.scan.return_value.to_arrow.return_value.to_pylist.return_value = [
        dict(agency_id="demo", schedule_version="v", manifest=json.dumps(schedule))
    ]
    empty = Mock()
    empty.current_snapshot.return_value = None
    catalog = Mock()
    catalog.load_table.side_effect = lambda name: (
        schedules if name == "gtfs.schedule_versions" else empty
    )
    monkeypatch.setattr(lake, "catalog", lambda: catalog)
    manifest = pin_coverage(tmp_path / "pin.json")
    assert manifest["snapshots"]["enriched_events"] is None
    result = report(manifest, "demo", "v", "2026-09-10", "2026-09-10T16:00:00Z")
    assert result["routes"][0]["missing_telemetry_trip_count"] == 3
    schedules.scan.assert_called_once_with(snapshot_id=123)
    empty.scan.assert_not_called()
    with pytest.raises(FileExistsError):
        write_report(tmp_path / "pin.json", {})


def test_report_rejects_as_of_after_snapshot_collection():
    with pytest.raises(ValueError, match="after snapshot collection"):
        report(
            {"created_at": "2026-09-10T15:00:00Z"},
            "demo",
            "v",
            "2026-09-10",
            "2026-09-10T16:00:00Z",
        )


def test_coverage_can_pin_before_enriched_table_exists(tmp_path, monkeypatch):
    from gtfs_lakehouse import lake
    from pyiceberg.exceptions import NoSuchTableError

    schedules = Mock()
    schedules.current_snapshot.return_value = SimpleNamespace(snapshot_id=123)
    client = Mock()
    client.load_table.side_effect = [schedules, NoSuchTableError("not created")]
    monkeypatch.setattr(lake, "catalog", lambda: client)
    result = pin_coverage(tmp_path / "pin.json")
    assert result["snapshots"] == {"schedule_versions": 123, "enriched_events": None}


def test_excluded_evidence_is_deduplicated_and_conflicts_across_trips_fail():
    manifest, events = inputs()
    different = manifest | {"schedule_version": "other"}
    assert coverage(different, events * 2, "2026-09-10", "2026-09-10T16:00:00Z")[
        "excluded_event_counts"
    ] == {"schedule_version": 4}
    with pytest.raises(ValueError, match="conflicting"):
        coverage(
            manifest,
            events + [events[0] | {"trip_id": "T2"}],
            "2026-09-10",
            "2026-09-10T16:00:00Z",
        )
