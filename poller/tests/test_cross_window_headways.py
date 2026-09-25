import hashlib

from gtfs_lakehouse.oracle import aggregate


def visit(identity, trip, observed, **changes):
    return (
        dict(
            event_id=identity,
            observed_at=observed,
            agency_id="demo",
            route_id="R1",
            direction_id=0,
            service_date="2026-09-10",
            trip_id=trip,
            vehicle_id="V1",
            payload=dict(
                trip={},
                trip_update=None,
                vehicle_position=dict(current_status="STOPPED_AT", stop_id="S1"),
            ),
        )
        | changes
    )


def test_cross_window_headways_include_predecessor_provenance_and_suppress_dwell():
    events = [
        visit("a", "T1", 290000),
        visit("dwell", "T1", 305000),
        visit("b", "T2", 310000),
        visit("c", "T3", 910000),
        visit("d", "T4", 910000),
    ]
    rows = aggregate(events, "live-v3")
    assert [row["arrival_count"] for row in rows] == [1, 1, 2]
    assert [row["headway_count"] for row in rows] == [0, 1, 2]
    assert [row["mean_headway_seconds"] for row in rows] == [None, 20, 300]
    assert rows[1]["context_event_count"] == 1
    assert rows[1]["input_digest"] == hashlib.sha256(b"a\nb\ndwell").hexdigest()
    assert rows[2]["input_digest"] == hashlib.sha256(b"a\nb\nc\nd").hexdigest()
    assert aggregate(list(reversed(events)) + events, "live-v3") == rows
    assert aggregate(events, "live-v2")[1]["arrival_count"] == 2


def test_gap_limit_and_route_direction_service_date_isolation():
    first = visit("a", "T1", 290000)
    for changes in (
        {"route_id": "R2"},
        {"direction_id": 1},
        {"service_date": "2026-09-11"},
    ):
        rows = aggregate([first, visit("b", "T2", 310000, **changes)], "live-v3")
        assert all(row["headway_count"] == 0 for row in rows)
    rows = aggregate([first, visit("b", "T2", 7_490_001)], "live-v3")
    assert rows[-1]["mean_headway_seconds"] is None
    boundary = aggregate([first, visit("b", "T2", 7_490_000)], "live-v3")
    assert boundary[-1]["mean_headway_seconds"] == 7200


def test_shared_golden_windows():
    import json
    from pathlib import Path

    fixture = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "tests/fixtures/cross_window_headways.json"
        ).read_text()
    )
    rows = aggregate(fixture["events"], "live-v3")
    assert [
        {key: row[key] for key in expected}
        for row, expected in zip(rows, fixture["expected"], strict=True)
    ] == fixture["expected"]


def test_replay_preserves_cross_window_context_and_rejects_missing_inputs(monkeypatch):
    import pytest
    from gtfs_lakehouse import replay

    events = [visit("a", "T1", 290000), visit("b", "T2", 310000)]
    live = aggregate(events, "live-v3")
    ledger = [event | {"generation": "live-v3"} for event in events]
    data = {"route_window_metrics": live, "metric_inputs": ledger}
    written = []
    monkeypatch.setattr(replay, "records", lambda manifest, name: data[name])
    monkeypatch.setattr(replay, "bootstrap", lambda: None)
    monkeypatch.setattr(replay, "insert", lambda rows: written.extend(rows))
    monkeypatch.setattr(replay, "latest", lambda generation: written)
    manifest = {"source_generation": "live-v3", "snapshots": {"metric_inputs": 1}}
    assert replay.rebuild(manifest, "replay-cross-window")["parity"]
    assert written[1]["mean_headway_seconds"] == 20
    assert written[1]["input_digest"] == live[1]["input_digest"]
    written.clear()
    data["metric_inputs"] = ledger[1:]
    with pytest.raises(ValueError, match="parity failed"):
        replay.rebuild(manifest, "replay-missing")
    assert written == []
