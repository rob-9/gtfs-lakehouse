import asyncio
import math
from dataclasses import replace
from unittest.mock import Mock

import httpx
import pytest
from prometheus_client import CollectorRegistry, generate_latest

from gtfs_lakehouse import ingestion
from gtfs_lakehouse.fixtures import realtime
from gtfs_lakehouse.models import RawSnapshot
from gtfs_lakehouse.poller_metrics import PollerMetrics
from gtfs_lakehouse.polling import PollResult, PollState


ENTRY = {"agency_id": "a", "feed_id": "f", "url": "https://example.test?token=secret"}
SNAPSHOT = RawSnapshot(
    "id", "a", "f", 123000, "https://example.test", 200, None, "etag", None, b"raw"
)


def sample(registry, name, **labels):
    return registry.get_sample_value(name, {"agency": "a", "feed": "f", **labels})


def run_once(tmp_path, metrics):
    asyncio.run(ingestion.poll_source(ENTRY, tmp_path, asyncio.Event(), True, metrics))


@pytest.mark.parametrize(
    "body, observed_at", [(realtime(100), 100000), (b"invalid", None)]
)
def test_publication_reports_source_time_only_for_valid_events(body, observed_at):
    publisher = ingestion.Publisher.__new__(ingestion.Publisher)
    publisher.objects = Mock()
    publisher.producer = Mock()
    publisher.producer.flush.return_value = 0
    report = publisher.publish(replace(SNAPSHOT, body=body))
    assert report["newest_observed_at"] == observed_at
    publisher.producer.commit_transaction.assert_called_once()


def test_failed_publication_preserves_backlog_and_restores_before_fetch(
    tmp_path, monkeypatch
):
    registry = CollectorRegistry()
    metrics = PollerMetrics(registry)

    async def fetch(*args):
        return PollResult(SNAPSHOT, PollState("etag"))

    class Publisher:
        def __init__(self, identity):
            pass

        def publish(self, snapshot):
            raise RuntimeError("publication unavailable")

    monkeypatch.setattr(ingestion, "fetch_feed", fetch)
    monkeypatch.setattr(ingestion, "Publisher", Publisher)
    with pytest.raises(RuntimeError, match="unavailable"):
        run_once(tmp_path, metrics)

    assert sample(registry, "gtfs_poll_attempts_total") == 1
    assert sample(registry, "gtfs_poll_responses_total", status="200") == 1
    assert sample(registry, "gtfs_poller_errors_total", stage="publish") == 1
    assert sample(registry, "gtfs_outbox_pending_snapshots") == 1
    assert sample(registry, "gtfs_outbox_pending_since_timestamp_seconds") == 123
    assert sample(registry, "gtfs_poller_published_snapshots_total") == 0
    assert math.isnan(sample(registry, "gtfs_feed_observed_timestamp_seconds"))

    # A restarted process must expose the durable backlog even if Kafka initialization fails.
    registry = CollectorRegistry()
    metrics = PollerMetrics(registry)

    def unavailable(identity):
        assert sample(registry, "gtfs_outbox_pending_snapshots") == 1
        raise RuntimeError("initialization unavailable")

    monkeypatch.setattr(ingestion, "Publisher", unavailable)
    with pytest.raises(RuntimeError, match="initialization"):
        run_once(tmp_path, metrics)
    assert sample(registry, "gtfs_poller_errors_total", stage="initialize") == 1

    async def unexpected_fetch(*args):
        pytest.fail("recovered outbox must publish before fetching")

    def publish(self, snapshot):
        assert snapshot == SNAPSHOT
        return {"newest_observed_at": 100000}

    monkeypatch.setattr(ingestion, "fetch_feed", unexpected_fetch)
    monkeypatch.setattr(Publisher, "publish", publish)
    monkeypatch.setattr(ingestion, "Publisher", Publisher)
    run_once(tmp_path, metrics)

    assert sample(registry, "gtfs_poll_attempts_total") == 0
    assert sample(registry, "gtfs_outbox_pending_snapshots") == 0
    assert math.isnan(sample(registry, "gtfs_outbox_pending_since_timestamp_seconds"))
    assert sample(registry, "gtfs_poller_published_snapshots_total") == 1
    assert sample(registry, "gtfs_feed_observed_timestamp_seconds") == 100
    box = ingestion.Outbox(next(tmp_path.glob("*.sqlite")))
    assert box.pending() is None
    assert box.state() == PollState("etag")
    box.db.close()


def test_304_refreshes_poll_health_but_not_source_freshness(tmp_path, monkeypatch):
    registry = CollectorRegistry()
    metrics = PollerMetrics(registry)
    stop = asyncio.Event()
    calls = 0

    async def fetch(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return PollResult(SNAPSHOT, PollState("etag"))
        assert sample(registry, "gtfs_feed_observed_timestamp_seconds") == 100
        stop.set()
        return PollResult(None, PollState("etag"))

    class Publisher:
        def __init__(self, identity):
            pass

        def publish(self, snapshot):
            return {"newest_observed_at": 100000}

    monkeypatch.setattr(ingestion, "fetch_feed", fetch)
    monkeypatch.setattr(ingestion, "Publisher", Publisher)
    monkeypatch.setattr(ingestion.time, "time", lambda: 200)
    asyncio.run(
        ingestion.poll_source(
            ENTRY | {"interval_seconds": 0.001}, tmp_path, stop, metrics=metrics
        )
    )
    assert sample(registry, "gtfs_poll_attempts_total") == 2
    assert sample(registry, "gtfs_poll_responses_total", status="304") == 1
    assert sample(registry, "gtfs_poll_last_success_timestamp_seconds") == 200
    assert sample(registry, "gtfs_feed_observed_timestamp_seconds") == 100
    assert sample(registry, "gtfs_poller_published_snapshots_total") == 1
    assert b"secret" not in generate_latest(registry)
    assert b"example.test" not in generate_latest(registry)


def test_fetch_failure_does_not_hide_another_feeds_success(tmp_path, monkeypatch):
    registry = CollectorRegistry()
    metrics = PollerMetrics(registry)

    async def fetch(client, config, state):
        if config.feed_id == "f":
            raise httpx.ConnectError("secret")
        return PollResult(None, PollState())

    monkeypatch.setattr(ingestion, "fetch_feed", fetch)
    monkeypatch.setattr(ingestion, "Publisher", lambda identity: object())

    async def run():
        return await asyncio.gather(
            ingestion.poll_source(ENTRY, tmp_path, asyncio.Event(), True, metrics),
            ingestion.poll_source(
                ENTRY | {"feed_id": "healthy"}, tmp_path, asyncio.Event(), True, metrics
            ),
            return_exceptions=True,
        )

    results = asyncio.run(run())
    assert isinstance(results[0], httpx.ConnectError)
    assert results[1] is None
    assert sample(registry, "gtfs_poller_errors_total", stage="fetch") == 1
    assert math.isnan(sample(registry, "gtfs_poll_last_success_timestamp_seconds"))
    assert (
        registry.get_sample_value(
            "gtfs_poll_responses_total",
            {"agency": "a", "feed": "healthy", "status": "304"},
        )
        == 1
    )
