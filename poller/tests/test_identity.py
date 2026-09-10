from gtfs_lakehouse.identity import canonical_json, event_id, payload_hash, snapshot_id


def test_canonical_json_ignores_mapping_order() -> None:
    left = {"position": {"longitude": -117.2, "latitude": 33.7}, "stop": None}
    right = {"stop": None, "position": {"latitude": 33.7, "longitude": -117.2}}

    assert canonical_json(left) == canonical_json(right)
    assert payload_hash(left) == payload_hash(right)


def test_event_id_changes_with_payload_or_observation_time() -> None:
    base = {
        "agency_id": "demo",
        "feed_id": "vehicles",
        "entity_type": "vehicle_position",
        "entity_id": "vehicle-1",
        "observed_at": 1_700_000_000_000,
        "payload_digest": payload_hash({"latitude": 33.7}),
    }

    original = event_id(**base)
    assert original == event_id(**base)
    assert original != event_id(**(base | {"observed_at": base["observed_at"] + 1_000}))
    assert original != event_id(
        **(base | {"payload_digest": payload_hash({"latitude": 33.8})})
    )


def test_snapshot_id_is_scoped_to_feed() -> None:
    assert snapshot_id(feed_id="a", body=b"same") != snapshot_id(
        feed_id="b", body=b"same"
    )
