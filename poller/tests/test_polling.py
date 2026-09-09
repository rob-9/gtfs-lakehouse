import asyncio

import httpx

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
            first = await fetch_feed(client, config, PollState(), fetched_at=1000)
            assert first.snapshot is not None
            assert first.snapshot.body == b"protobuf"
            second = await fetch_feed(client, config, first.state, fetched_at=2000)
            assert second.snapshot is None

    asyncio.run(run())
    assert len(requests) == 2

