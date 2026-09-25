"""Process-local polling metrics with bounded, credential-free labels."""

from prometheus_client import REGISTRY, Counter, Gauge


class PollerMetrics:
    def __init__(self, registry=REGISTRY):
        labels = ["agency", "feed"]

        def counter(name, description, extra=()):
            return Counter(name, description, labels + list(extra), registry=registry)

        def gauge(name, description):
            return Gauge(name, description, labels, registry=registry)

        self.attempts = counter("gtfs_poll_attempts_total", "HTTP feed fetch attempts")
        self.responses = counter(
            "gtfs_poll_responses_total", "Successful HTTP fetches", ["status"]
        )
        self.errors = counter(
            "gtfs_poller_errors_total", "Failed polling cycles by stage", ["stage"]
        )
        self.published = counter(
            "gtfs_poller_published_snapshots_total",
            "Snapshot publications acknowledged locally; retries can count twice",
        )
        self.last_success = gauge(
            "gtfs_poll_last_success_timestamp_seconds",
            "Last successful HTTP fetch, including 304; NaN until first success",
        )
        self.pending = gauge(
            "gtfs_outbox_pending_snapshots", "Durable snapshots awaiting publication"
        )
        self.pending_since = gauge(
            "gtfs_outbox_pending_since_timestamp_seconds",
            "Pending snapshot receipt time; NaN when the outbox is empty",
        )
        self.observed_at = gauge(
            "gtfs_feed_observed_timestamp_seconds",
            "Newest event time in the last acknowledged snapshot; NaN when unknown",
        )

    def initialize(self, source):
        self.attempts.labels(*source)
        self.published.labels(*source)
        for status in ("200", "304"):
            self.responses.labels(*source, status)
        for stage in ("initialize", "fetch", "stage", "publish", "acknowledge"):
            self.errors.labels(*source, stage)
        self.last_success.labels(*source).set(float("nan"))
        self.observed_at.labels(*source).set(float("nan"))

    def outbox(self, source, snapshot):
        self.pending.labels(*source).set(int(snapshot is not None))
        self.pending_since.labels(*source).set(
            snapshot.fetched_at / 1000 if snapshot is not None else float("nan")
        )
