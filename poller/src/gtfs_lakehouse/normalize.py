"""Convert GTFS Realtime protobuf entities into stable event envelopes."""

from __future__ import annotations

from typing import Any

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from .identity import event_id, payload_hash, snapshot_id
from .models import DeadLetter, NormalizedEvent


def _text(value: str) -> str | None:
    return value or None


def _optional(message: Any, field: str) -> Any | None:
    return getattr(message, field) if message.HasField(field) else None


def _enum_name(enum: Any, value: int | None) -> str | None:
    return enum.Name(value) if value is not None else None


def _vehicle_payload(vehicle: Any) -> dict[str, Any]:
    if not vehicle.HasField("position"):
        raise ValueError("vehicle position is missing coordinates")

    latitude = vehicle.position.latitude
    longitude = vehicle.position.longitude
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("vehicle coordinates are outside valid ranges")

    return {
        "vehicle_position": {
            "latitude": latitude,
            "longitude": longitude,
            "bearing": _optional(vehicle.position, "bearing"),
            "speed": _optional(vehicle.position, "speed"),
            "stop_id": _text(vehicle.stop_id),
            "current_stop_sequence": _optional(vehicle, "current_stop_sequence"),
            "current_status": _enum_name(
                gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus,
                _optional(vehicle, "current_status"),
            ),
        },
        "trip_update": None,
    }


def _stop_time_event(event: Any) -> tuple[int | None, int | None]:
    if event is None:
        return None, None
    return _optional(event, "delay"), _optional(event, "time")


def _trip_update_payload(update: Any) -> dict[str, Any]:
    stops = []
    for item in update.stop_time_update:
        arrival_delay, arrival_time = _stop_time_event(
            item.arrival if item.HasField("arrival") else None
        )
        departure_delay, departure_time = _stop_time_event(
            item.departure if item.HasField("departure") else None
        )
        stops.append(
            {
                "stop_sequence": _optional(item, "stop_sequence"),
                "stop_id": _text(item.stop_id),
                "arrival_delay": arrival_delay,
                "arrival_time": arrival_time,
                "departure_delay": departure_delay,
                "departure_time": departure_time,
                "schedule_relationship": _enum_name(
                    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship,
                    _optional(item, "schedule_relationship"),
                ),
            }
        )
    return {"vehicle_position": None, "trip_update": {"stop_time_updates": stops}}


def _dead_letter(
    *,
    agency_id: str,
    feed_id: str,
    snapshot_digest: str,
    entity_id: str | None,
    ingested_at: int,
    reason: str,
    detail: str,
) -> DeadLetter:
    return DeadLetter(
        agency_id=agency_id,
        feed_id=feed_id,
        snapshot_id=snapshot_digest,
        entity_id=entity_id,
        ingested_at=ingested_at,
        reason=reason,
        detail=detail,
    )


def normalize_feed(
    body: bytes,
    *,
    agency_id: str,
    feed_id: str,
    ingested_at: int,
) -> tuple[list[NormalizedEvent], list[DeadLetter]]:
    """Normalize supported entities and return structured failures separately."""
    digest = snapshot_id(feed_id=feed_id, body=body)
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(body)
    except DecodeError as exc:
        return [], [
            _dead_letter(
                agency_id=agency_id,
                feed_id=feed_id,
                snapshot_digest=digest,
                entity_id=None,
                ingested_at=ingested_at,
                reason="protobuf_decode_error",
                detail=str(exc),
            )
        ]

    header_seconds = _optional(message.header, "timestamp")
    header_millis = header_seconds * 1000 if header_seconds else None
    events: list[NormalizedEvent] = []
    failures: list[DeadLetter] = []

    for entity in message.entity:
        entity_id_value = _text(entity.id)
        try:
            if entity.is_deleted:
                raise ValueError("deleted entities are not supported")
            if not entity_id_value:
                raise ValueError("entity ID is missing")

            supported = [entity.HasField("vehicle"), entity.HasField("trip_update")]
            if sum(supported) != 1:
                raise ValueError("entity must contain exactly one supported payload")

            if entity.HasField("vehicle"):
                source = entity.vehicle
                entity_type = "vehicle_position"
                payload = _vehicle_payload(source)
            else:
                source = entity.trip_update
                entity_type = "trip_update"
                payload = _trip_update_payload(source)

            entity_seconds = _optional(source, "timestamp")
            if entity_seconds:
                observed_at = entity_seconds * 1000
                timestamp_source = "entity"
            elif header_millis is not None:
                observed_at = header_millis
                timestamp_source = "feed_header"
            else:
                raise ValueError("entity and feed header timestamps are missing")

            trip = source.trip if source.HasField("trip") else None
            vehicle = source.vehicle if source.HasField("vehicle") else None
            payload_digest = payload_hash(payload)
            events.append(
                NormalizedEvent(
                    schema_version=1,
                    event_id=event_id(
                        agency_id=agency_id,
                        entity_type=entity_type,
                        entity_id=entity_id_value,
                        observed_at=observed_at,
                        payload_digest=payload_digest,
                    ),
                    agency_id=agency_id,
                    feed_id=feed_id,
                    entity_type=entity_type,
                    entity_id=entity_id_value,
                    trip_id=_text(trip.trip_id) if trip else None,
                    route_id=_text(trip.route_id) if trip else None,
                    vehicle_id=_text(vehicle.id) if vehicle else None,
                    observed_at=observed_at,
                    feed_generated_at=header_millis,
                    ingested_at=ingested_at,
                    timestamp_source=timestamp_source,
                    schedule_version=None,
                    payload_hash=payload_digest,
                    payload=payload,
                )
            )
        except (TypeError, ValueError) as exc:
            failures.append(
                _dead_letter(
                    agency_id=agency_id,
                    feed_id=feed_id,
                    snapshot_digest=digest,
                    entity_id=entity_id_value,
                    ingested_at=ingested_at,
                    reason="validation_error",
                    detail=str(exc),
                )
            )

    return events, failures

