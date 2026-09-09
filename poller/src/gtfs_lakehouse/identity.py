"""Canonical serialization and content-derived identifiers."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> bytes:
    """Serialize JSON-compatible data identically across runs."""
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def payload_hash(payload: dict[str, Any]) -> str:
    return sha256_hex(canonical_json(payload))


def event_id(
    *,
    agency_id: str,
    entity_type: str,
    entity_id: str,
    observed_at: int,
    payload_digest: str,
) -> str:
    identity = [agency_id, entity_type, entity_id, observed_at, payload_digest]
    return sha256_hex(canonical_json(identity))


def snapshot_id(*, feed_id: str, body: bytes) -> str:
    return sha256_hex(feed_id.encode("utf-8") + b"\0" + body)

