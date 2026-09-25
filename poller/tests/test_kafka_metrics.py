from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry, generate_latest

from gtfs_lakehouse.kafka_metrics import KafkaLagCollector, offsets


def consumer(offset=12, low=10, high=30):
    result = Mock()
    result.list_topics.return_value = SimpleNamespace(
        topics={"events": SimpleNamespace(error=None, partitions={0: object()})}
    )
    result.committed.return_value = [TopicPartition("events", 0, offset)]
    result.get_watermark_offsets.return_value = (low, high)
    return result


@pytest.mark.parametrize(
    "offset, lag, known",
    [(12, 18, True), (-1001, 20, False), (2, 20, True), (50, 0, True)],
)
def test_lag_accounts_for_retention_and_missing_or_invalid_commits(offset, lag, known):
    client = consumer(offset)
    assert offsets(client, ["events"]) == [("events", 0, 10, 30, offset, known, lag)]
    client.subscribe.assert_not_called()
    client.assign.assert_not_called()
    client.commit.assert_not_called()


def test_group_failure_drops_samples_without_hiding_healthy_groups():
    bad, good = consumer(), consumer()
    bad.list_topics.side_effect = RuntimeError("broker unavailable")
    configs = []

    def factory(config):
        configs.append(config)
        return bad if config["group.id"] == "bad" else good

    registry = CollectorRegistry()
    registry.register(
        KafkaLagCollector(factory, {"bad": ("events",), "good": ("events",)})
    )
    body = generate_latest(registry).decode()
    assert 'gtfs_kafka_group_collection_up{group="bad"} 0.0' in body
    assert (
        'gtfs_kafka_consumer_lag{group="good",partition="0",topic="events"} 18.0'
        in body
    )
    assert 'gtfs_kafka_consumer_lag{group="bad"' not in body
    assert all(
        not config["enable.auto.commit"] and not config["allow.auto.create.topics"]
        for config in configs
    )
    bad.close.assert_called_once()
    good.close.assert_called_once()
    good.list_topics.side_effect = RuntimeError("now unavailable")
    assert "gtfs_kafka_consumer_lag{" not in generate_latest(registry).decode()


def test_missing_topic_and_deadline_fail_instead_of_reporting_zero(monkeypatch):
    with pytest.raises(RuntimeError, match="metadata"):
        offsets(consumer(), ["missing"])
    ticks = iter([0, 3])
    monkeypatch.setattr("gtfs_lakehouse.kafka_metrics.monotonic", lambda: next(ticks))
    with pytest.raises(TimeoutError):
        offsets(consumer(), ["events"], timeout=2)


def test_close_failure_marks_group_unhealthy_instead_of_breaking_scrape():
    client = consumer()
    client.close.side_effect = RuntimeError("close failed")
    registry = CollectorRegistry()
    registry.register(KafkaLagCollector(lambda config: client, {"test": ("events",)}))
    body = generate_latest(registry).decode()
    assert 'gtfs_kafka_group_collection_up{group="test"} 0.0' in body
    assert "gtfs_kafka_consumer_lag{" not in body
