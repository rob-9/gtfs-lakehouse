"""Read-only local exporter for Flink health and serving visibility."""

import time
import os

import httpx
from prometheus_client import Gauge, start_http_server

from .serving import latest


def monitor(port=9108):
    flink_url = os.getenv("FLINK_URL", "http://localhost:8081")
    up = Gauge("gtfs_exporter_up", "Whether the most recent collection succeeded")
    running = Gauge("gtfs_flink_running_jobs", "Running Flink jobs")
    checkpoints = Gauge(
        "gtfs_flink_completed_checkpoints", "Completed checkpoints", ["job"]
    )
    failures = Gauge("gtfs_flink_failed_checkpoints", "Failed checkpoints", ["job"])
    duration = Gauge(
        "gtfs_flink_checkpoint_duration_ms",
        "Latest completed checkpoint duration",
        ["job"],
    )
    windows = Gauge("gtfs_serving_windows", "Queryable final route windows")
    predictions = Gauge(
        "gtfs_serving_prediction_samples",
        "Reported prediction samples in final windows",
    )
    start_http_server(port, addr="0.0.0.0")
    while True:
        try:
            response = httpx.get(f"{flink_url}/jobs/overview", timeout=5)
            response.raise_for_status()
            jobs = response.json()["jobs"]
            running.set(sum(job["state"] == "RUNNING" for job in jobs))
            for job in jobs:
                stats = httpx.get(
                    f"{flink_url}/jobs/{job['jid']}/checkpoints", timeout=5
                ).json()
                checkpoints.labels(job["jid"]).set(
                    stats.get("counts", {}).get("completed", 0)
                )
                failures.labels(job["jid"]).set(
                    stats.get("counts", {}).get("failed", 0)
                )
                completed = stats.get("latest", {}).get("completed") or {}
                duration.labels(job["jid"]).set(completed.get("end_to_end_duration", 0))
            rows = latest()
            windows.set(len(rows))
            predictions.set(sum(row["prediction_count"] for row in rows))
            up.set(1)
        except Exception:
            up.set(0)
        time.sleep(5)
