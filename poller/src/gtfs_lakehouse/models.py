"""Immutable records passed between polling, normalization, and publication."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class RawSnapshot:
    snapshot_id: str
    agency_id: str
    feed_id: str
    fetched_at: int
    url: str
    status_code: int
    content_type: str | None
    etag: str | None
    last_modified: str | None
    body: bytes


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    schema_version: int
    event_id: str
    agency_id: str
    feed_id: str
    entity_type: Literal["vehicle_position", "trip_update"]
    entity_id: str
    trip_id: str | None
    route_id: str | None
    vehicle_id: str | None
    observed_at: int
    feed_generated_at: int | None
    ingested_at: int
    timestamp_source: Literal["entity", "feed_header"]
    schedule_version: None
    payload_hash: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DeadLetter:
    agency_id: str
    feed_id: str
    snapshot_id: str
    entity_id: str | None
    ingested_at: int
    reason: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
