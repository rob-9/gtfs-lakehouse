"""Conditional HTTP polling without publication side effects."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from .identity import snapshot_id
from .models import RawSnapshot


@dataclass(frozen=True, slots=True)
class FeedConfig:
    agency_id: str
    feed_id: str
    url: str
    timeout_seconds: float = 15.0


@dataclass(frozen=True, slots=True)
class PollState:
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class PollResult:
    snapshot: RawSnapshot | None
    state: PollState


async def fetch_feed(
    client: httpx.AsyncClient,
    config: FeedConfig,
    state: PollState,
    *,
    fetched_at: int,
) -> PollResult:
    headers = {"Accept": "application/x-protobuf"}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    response = await client.get(
        config.url,
        headers=headers,
        timeout=config.timeout_seconds,
        follow_redirects=True,
    )
    if response.status_code == 304:
        return PollResult(snapshot=None, state=state)
    response.raise_for_status()

    next_state = PollState(
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )
    snapshot = RawSnapshot(
        snapshot_id=snapshot_id(feed_id=config.feed_id, body=response.content),
        agency_id=config.agency_id,
        feed_id=config.feed_id,
        fetched_at=fetched_at,
        url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        etag=next_state.etag,
        last_modified=next_state.last_modified,
        body=response.content,
    )
    return PollResult(snapshot=snapshot, state=next_state)

