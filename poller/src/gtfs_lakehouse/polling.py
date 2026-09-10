"""Conditional HTTP polling without publication side effects."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from time import time_ns
from urllib.parse import urlsplit, urlunsplit

import httpx

from .identity import snapshot_id
from .models import RawSnapshot


@dataclass(frozen=True, slots=True)
class FeedConfig:
    agency_id: str
    feed_id: str
    url: str
    timeout_seconds: float = 15.0
    max_response_bytes: int = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PollState:
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class PollResult:
    """Adopt state only after the snapshot and its outputs are durably acknowledged."""

    snapshot: RawSnapshot | None
    state: PollState


async def fetch_feed(
    client: httpx.AsyncClient,
    config: FeedConfig,
    state: PollState,
    *,
    clock: Callable[[], int] = lambda: time_ns() // 1_000_000,
) -> PollResult:
    headers = {"Accept": "application/x-protobuf"}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    async with client.stream(
        "GET",
        config.url,
        headers=headers,
        timeout=config.timeout_seconds,
        follow_redirects=False,
    ) as response:
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > config.max_response_bytes:
                raise ValueError("feed exceeds configured response size limit")
        content = bytes(body)
    if response.status_code == 304:
        return PollResult(
            snapshot=None,
            state=PollState(
                etag=response.headers.get("etag", state.etag),
                last_modified=response.headers.get(
                    "last-modified", state.last_modified
                ),
            ),
        )
    response.raise_for_status()
    if response.status_code != 200:
        raise httpx.HTTPStatusError(
            "expected a complete feed response (200)",
            request=response.request,
            response=response,
        )
    fetched_at = clock()

    next_state = PollState(
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )
    snapshot = RawSnapshot(
        snapshot_id=snapshot_id(feed_id=config.feed_id, body=content),
        agency_id=config.agency_id,
        feed_id=config.feed_id,
        fetched_at=fetched_at,
        url=redact_url(str(response.url)),
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        etag=next_state.etag,
        last_modified=next_state.last_modified,
        body=content,
    )
    return PollResult(snapshot=snapshot, state=next_state)


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(
        (parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, "", "")
    )
