"""Read-only Kafka committed-offset monitoring; never joins consumer groups."""

from concurrent.futures import ThreadPoolExecutor
from time import monotonic

from confluent_kafka import Consumer, TopicPartition
from prometheus_client.core import GaugeMetricFamily

from .services import kafka_address

GROUPS = {
    "gtfs-normalization-v1": ("gtfs.normalized.events",),
    "gtfs-clickhouse-v1": ("gtfs.route.metrics",),
    "gtfs-raw-archive-v1": ("gtfs.raw.snapshots",),
    "gtfs-audit-archive-v1": ("gtfs.dead_letter", "gtfs.late.events"),
    "gtfs-schedules-v1": ("gtfs.schedule.versions",),
}


def offsets(consumer, topics, timeout=2):
    # Fetch all metadata, rather than requesting a missing topic by name (which
    # can auto-create it on some brokers). Never subscribe, assign, or commit.
    deadline = monotonic() + timeout

    def remaining():
        budget = deadline - monotonic()
        if budget <= 0:
            raise TimeoutError("offset collection deadline exceeded")
        return budget

    metadata = consumer.list_topics(timeout=remaining())
    partitions = []
    for topic in topics:
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            raise RuntimeError("topic metadata unavailable")
        partitions.extend(TopicPartition(topic, number) for number in info.partitions)
    result = []
    for partition in consumer.committed(partitions, timeout=remaining()):
        if partition.error is not None:
            raise RuntimeError("committed offset unavailable")
        low, high = consumer.get_watermark_offsets(
            partition, timeout=remaining(), cached=False
        )
        committed = partition.offset
        known = committed >= 0
        # All monitored groups start at earliest. Missing commits mean retained
        # backlog, while an out-of-range commit is exposed separately.
        effective = max(low, min(high, committed)) if known else low
        result.append(
            (
                partition.topic,
                partition.partition,
                low,
                high,
                committed,
                known,
                high - effective,
            )
        )
    return result


class KafkaLagCollector:
    """Collect each group independently and omit stale samples on errors."""

    def __init__(self, consumer_factory=Consumer, groups=None):
        self.consumer_factory = consumer_factory
        self.groups = GROUPS if groups is None else groups

    def describe(self):
        for name in (
            "gtfs_kafka_group_collection_up",
            "gtfs_kafka_consumer_lag",
            "gtfs_kafka_committed_offset",
            "gtfs_kafka_committed_offset_known",
            "gtfs_kafka_committed_offset_out_of_range",
        ):
            yield GaugeMetricFamily(name, "Kafka offset collection")

    def collect(self):
        up = GaugeMetricFamily(
            "gtfs_kafka_group_collection_up",
            "Offset collection succeeded",
            labels=["group"],
        )
        labels = ["group", "topic", "partition"]
        lag = GaugeMetricFamily(
            "gtfs_kafka_consumer_lag",
            "Log-end minus bounded committed offset; includes transactional/control offsets",
            labels=labels,
        )
        committed = GaugeMetricFamily(
            "gtfs_kafka_committed_offset",
            "Committed next offset; -1 when absent",
            labels=labels,
        )
        known = GaugeMetricFamily(
            "gtfs_kafka_committed_offset_known",
            "Whether the group has a committed offset",
            labels=labels,
        )
        invalid = GaugeMetricFamily(
            "gtfs_kafka_committed_offset_out_of_range",
            "Committed offset outside retained log range",
            labels=labels,
        )

        def read(item):
            group, topics = item
            consumer = None
            rows = None
            try:
                consumer = self.consumer_factory(
                    {
                        "bootstrap.servers": kafka_address(),
                        "group.id": group,
                        "enable.auto.commit": False,
                        "enable.auto.offset.store": False,
                        "allow.auto.create.topics": False,
                    }
                )
                rows = offsets(consumer, topics)
            except Exception:
                rows = None
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        rows = None
            return group, rows

        with ThreadPoolExecutor(max_workers=5) as pool:
            for group, rows in pool.map(read, self.groups.items()):
                up.add_metric([group], int(rows is not None))
                for topic, partition, low, high, offset, exists, backlog in rows or []:
                    key = [group, topic, str(partition)]
                    lag.add_metric(key, backlog)
                    committed.add_metric(key, offset if exists else -1)
                    known.add_metric(key, int(exists))
                    invalid.add_metric(key, int(exists and not low <= offset <= high))
        yield from (up, lag, committed, known, invalid)
