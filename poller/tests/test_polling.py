import asyncio

import httpx
import pytest

from gtfs_lakehouse.polling import FeedConfig, PollState, fetch_feed


def test_conditional_polling_tracks_validators() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return httpx.Response(
            200,
            content=b"protobuf",
            headers={"etag": '"v1"', "content-type": "application/x-protobuf"},
        )

    async def run() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            config = FeedConfig("demo", "vehicles", "https://example.test/vehicles.pb")
            first = await fetch_feed(client, config, PollState(), clock=lambda: 1000)
            assert first.snapshot is not None
            assert first.snapshot.body == b"protobuf"
            second = await fetch_feed(client, config, first.state, clock=lambda: 2000)
            assert second.snapshot is None

    asyncio.run(run())
    assert len(requests) == 2


def test_receipt_clock_runs_after_response_and_body_arrive():
    received = False

    def handler(request):
        nonlocal received
        received = True
        return httpx.Response(200, content=b"feed")

    def clock():
        assert received
        return 1234

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await fetch_feed(
                client,
                FeedConfig("a", "f", "https://example.test"),
                PollState(),
                clock=clock,
            )
            assert result.snapshot.fetched_at == 1234

    asyncio.run(run())


@pytest.mark.parametrize("status", [204, 206, 429, 500])
def test_incomplete_and_failed_responses_do_not_advance_state(status):
    async def run():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(status, headers={"etag": "new"})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            state = PollState(etag="old")
            with pytest.raises(httpx.HTTPStatusError):
                await fetch_feed(
                    client, FeedConfig("a", "f", "https://example.test"), state
                )
            assert state.etag == "old"

    asyncio.run(run())


def test_not_modified_refreshes_validators():
    async def run():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(304, headers={"etag": "new"})
        )
        async with httpx.AsyncClient(transport=transport) as client:
            result = await fetch_feed(
                client,
                FeedConfig("a", "f", "https://example.test"),
                PollState("old", "yesterday"),
            )
            assert result.snapshot is None
            assert result.state == PollState("new", "yesterday")

    asyncio.run(run())


def test_oversized_response_is_rejected_and_url_credentials_are_redacted():
    from gtfs_lakehouse.polling import redact_url

    assert (
        redact_url("https://user:secret@example.test/feed?token=private")
        == "https://example.test/feed"
    )

    async def run():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"too large")
        )
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(ValueError, match="size limit"):
                await fetch_feed(
                    client,
                    FeedConfig("a", "f", "https://example.test", max_response_bytes=3),
                    PollState(),
                )

    asyncio.run(run())
