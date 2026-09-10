from google.transit import gtfs_realtime_pb2
import pytest

from gtfs_lakehouse.normalize import normalize_feed


def _fixture() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000

    vehicle = feed.entity.add()
    vehicle.id = "vehicle-entity"
    vehicle.vehicle.trip.trip_id = "trip-1"
    vehicle.vehicle.trip.route_id = "route-1"
    vehicle.vehicle.vehicle.id = "bus-1"
    vehicle.vehicle.timestamp = 1_700_000_005
    vehicle.vehicle.position.latitude = 33.6846
    vehicle.vehicle.position.longitude = -117.8265

    update = feed.entity.add()
    update.id = "update-entity"
    update.trip_update.trip.trip_id = "trip-2"
    stop = update.trip_update.stop_time_update.add()
    stop.stop_sequence = 3
    stop.stop_id = "stop-3"
    stop.arrival.delay = 45
    return feed.SerializeToString(deterministic=True)


def test_normalization_is_deterministic_across_ingestion_attempts() -> None:
    body = _fixture()
    first, first_errors = normalize_feed(
        body, agency_id="demo", feed_id="vehicles", ingested_at=1_700_000_010_000
    )
    second, second_errors = normalize_feed(
        body, agency_id="demo", feed_id="vehicles", ingested_at=1_700_000_020_000
    )

    assert not first_errors
    assert not second_errors
    assert [item.event_id for item in first] == [item.event_id for item in second]
    assert [item.payload_hash for item in first] == [
        item.payload_hash for item in second
    ]
    assert first[0].timestamp_source == "entity"
    assert first[1].timestamp_source == "feed_header"
    assert (
        first[1].payload["trip_update"]["stop_time_updates"][0]["arrival_delay"] == 45
    )


def test_invalid_entity_is_dead_lettered_without_dropping_valid_entities() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    invalid = feed.entity.add()
    invalid.id = "missing-payload"
    valid = feed.entity.add()
    valid.id = "vehicle"
    valid.vehicle.position.latitude = 33.0
    valid.vehicle.position.longitude = -117.0

    events, failures = normalize_feed(
        feed.SerializeToString(),
        agency_id="demo",
        feed_id="vehicles",
        ingested_at=1_700_000_010_000,
    )

    assert [event.entity_id for event in events] == ["vehicle"]
    assert [failure.entity_id for failure in failures] == ["missing-payload"]
    assert failures[0].reason == "validation_error"


def test_corrupt_protobuf_has_snapshot_level_dead_letter() -> None:
    events, failures = normalize_feed(
        b"not protobuf",
        agency_id="demo",
        feed_id="vehicles",
        ingested_at=1_700_000_010_000,
    )

    assert not events
    assert failures[0].reason == "protobuf_decode_error"
    assert failures[0].entity_id is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("trip_id", "corrected-trip"),
        ("route_id", "corrected-route"),
        ("start_date", "20231114"),
        ("start_time", "25:15:00"),
        ("direction_id", 1),
        ("schedule_relationship", 3),
    ],
)
def test_trip_corrections_change_identity(field, value) -> None:
    feed = gtfs_realtime_pb2.FeedMessage.FromString(_fixture())
    original, _ = normalize_feed(
        _fixture(), agency_id="demo", feed_id="a", ingested_at=1
    )
    setattr(feed.entity[0].vehicle.trip, field, value)
    corrected, failures = normalize_feed(
        feed.SerializeToString(),
        agency_id="demo",
        feed_id="a",
        ingested_at=2,
    )
    assert not failures
    assert original[0].event_id != corrected[0].event_id
    assert original[0].payload_hash != corrected[0].payload_hash
    expected = "CANCELED" if field == "schedule_relationship" else value
    assert corrected[0].payload["trip"][field] == expected


def test_vehicle_correction_and_feed_scope_change_identity() -> None:
    feed = gtfs_realtime_pb2.FeedMessage.FromString(_fixture())
    original, _ = normalize_feed(
        _fixture(), agency_id="demo", feed_id="a", ingested_at=1
    )
    other_feed, _ = normalize_feed(
        _fixture(), agency_id="demo", feed_id="b", ingested_at=1
    )
    feed.entity[0].vehicle.vehicle.id = "replacement-bus"
    corrected, _ = normalize_feed(
        feed.SerializeToString(), agency_id="demo", feed_id="a", ingested_at=1
    )
    assert (
        len({original[0].event_id, other_feed[0].event_id, corrected[0].event_id}) == 3
    )


@pytest.mark.parametrize("body", [b"", b"\x0a\x00"])
def test_missing_header_is_audited(body) -> None:
    events, failures = normalize_feed(
        body, agency_id="demo", feed_id="a", ingested_at=1
    )
    assert not events
    assert len(failures) == 1
    assert failures[0].entity_id is None


def test_missing_coordinate_is_not_treated_as_zero() -> None:
    feed = gtfs_realtime_pb2.FeedMessage.FromString(_fixture())
    feed.entity[0].vehicle.position.ClearField("latitude")
    events, failures = normalize_feed(
        feed.SerializePartialToString(),
        agency_id="demo",
        feed_id="a",
        ingested_at=1,
    )
    assert [event.entity_type for event in events] == ["trip_update"]
    assert len(failures) == 1
    assert "latitude" in failures[0].detail


def test_differential_feed_is_rejected_as_a_whole() -> None:
    feed = gtfs_realtime_pb2.FeedMessage.FromString(_fixture())
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
    events, failures = normalize_feed(
        feed.SerializeToString(), agency_id="demo", feed_id="a", ingested_at=1
    )
    assert not events
    assert "differential" in failures[0].detail
