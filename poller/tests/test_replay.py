import pytest

from gtfs_lakehouse.fixtures import realtime, static_zip
from gtfs_lakehouse.normalize import normalize_feed
from gtfs_lakehouse.schedules import parse_archive, enrich
from gtfs_lakehouse.oracle import aggregate
from gtfs_lakehouse.replay import verified_migration_inputs


def test_legacy_migration_requires_exact_digest_and_values():
    events, _ = normalize_feed(realtime(), agency_id="demo", feed_id="f", ingested_at=1)
    schedule = parse_archive(static_zip())
    enriched = [enrich(event.to_dict(), schedule, "v") for event in events]
    metrics = aggregate(enriched)
    migrated = verified_migration_inputs(metrics, [], enriched, "live-v2")
    assert len(migrated) == 4
    assert (
        len(verified_migration_inputs(metrics + metrics, [], enriched, "live-v2")) == 4
    )
    assert all(
        row["admission_origin"] == "verified_legacy_migration" for row in migrated
    )
    assert verified_migration_inputs(metrics, migrated, enriched, "live-v2") == []
    extra = enriched[0] | {"event_id": "unexpected-input"}
    with pytest.raises(ValueError, match="cannot be verified"):
        verified_migration_inputs(metrics, [], enriched + [extra], "live-v2")
