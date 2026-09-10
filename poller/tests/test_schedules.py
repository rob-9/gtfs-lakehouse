from datetime import date

import pytest

from gtfs_lakehouse.fixtures import static_zip, realtime
from gtfs_lakehouse.normalize import normalize_feed
from gtfs_lakehouse.schedules import parse_archive, enrich, seconds, service_instant, active


def test_fixture_schedule_resolves_trip_and_cancellation():
    schedule = parse_archive(static_zip())
    events, errors = normalize_feed(realtime(), agency_id="demo", feed_id="f", ingested_at=1)
    assert not errors
    enriched = [enrich(event.to_dict(), schedule, "version") for event in events]
    assert all(row["unmatched_reason"] is None for row in enriched)
    assert all(row["route_id"] == "R1" for row in enriched)
    assert enriched[2]["payload"]["trip"]["schedule_relationship"] == "CANCELED"


def test_calendar_exceptions_override_regular_service():
    schedule = parse_archive(static_zip())
    day = date(2026, 9, 10)
    assert active(schedule, "daily", day)
    schedule["calendar_dates"] = [{"service_id": "daily", "date": "20260910", "exception_type": "2"}]
    assert not active(schedule, "daily", day)


def test_gtfs_clock_preserves_hours_beyond_midnight():
    assert seconds("25:15:00") == 90900
    for day in (date(2026, 3, 8), date(2026, 11, 1)):
        assert service_instant(day, "25:00:00", "America/Los_Angeles") - service_instant(day, "01:00:00", "America/Los_Angeles") == 86400
    with pytest.raises(ValueError):
        seconds("08:60:00")


def test_unknown_trip_is_preserved_with_reason():
    events, _ = normalize_feed(realtime(), agency_id="demo", feed_id="f", ingested_at=1)
    result = enrich(events[0].to_dict() | {"trip_id": "unknown"}, parse_archive(static_zip()), "v")
    assert result["unmatched_reason"] == "unknown_trip"
