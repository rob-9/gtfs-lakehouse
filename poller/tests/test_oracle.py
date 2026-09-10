from gtfs_lakehouse.fixtures import realtime, static_zip
from gtfs_lakehouse.normalize import normalize_feed
from gtfs_lakehouse.schedules import parse_archive, enrich
from gtfs_lakehouse.oracle import aggregate


def test_fixture_predictions_and_cancellation_are_not_observed_arrivals():
    events, _ = normalize_feed(
        realtime(), agency_id="demo", feed_id="realtime", ingested_at=1
    )
    schedule = parse_archive(static_zip())
    enriched = [enrich(event.to_dict(), schedule, "v") for event in events]
    (metric,) = aggregate(enriched)
    assert metric["prediction_count"] == 2
    assert metric["mean_delay_seconds"] == 90
    assert metric["p50_delay_seconds"] == 60
    assert metric["p95_delay_seconds"] == 120
    assert metric["canceled_trip_count"] == 1
    assert metric["arrival_count"] == 1
    assert metric["mean_headway_seconds"] is None
    assert aggregate(enriched + enriched) == [metric]
    assert aggregate(list(reversed(enriched))) == [metric]
