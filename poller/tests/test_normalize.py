from google.transit import gtfs_realtime_pb2

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
    assert [item.payload_hash for item in first] == [item.payload_hash for item in second]
    assert first[0].timestamp_source == "entity"
    assert first[1].timestamp_source == "feed_header"
    assert first[1].payload["trip_update"]["stop_time_updates"][0]["arrival_delay"] == 45


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

