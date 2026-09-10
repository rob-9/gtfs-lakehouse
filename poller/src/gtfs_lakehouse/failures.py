"""Explicitly scoped local failure injection with retained evidence."""

import json
import subprocess
import time
from pathlib import Path

import httpx

from .smoke import pipeline


def recover_taskmanager(output):
    destination = Path(output)
    destination.mkdir(parents=True, exist_ok=False)
    jobs = httpx.get("http://localhost:8081/jobs/overview", timeout=10).json()["jobs"]
    running = [job for job in jobs if job["state"] == "RUNNING"]
    if len(running) != 1:
        raise ValueError("failure test requires exactly one running local job")
    job_id = running[0]["jid"]
    url = f"http://localhost:8081/jobs/{job_id}/checkpoints"
    before = httpx.get(url, timeout=10).json()
    (destination / "before.json").write_text(json.dumps(before, indent=2))

    def interrupt_checkpoint():
        deadline = time.monotonic() + 30
        # Hold the worker so a checkpoint cannot finish between REST samples.
        subprocess.run(["docker", "compose", "pause", "taskmanager"], check=True)
        try:
            with httpx.Client(timeout=5) as client:
                client.post(url, json={}).raise_for_status()
                while time.monotonic() < deadline:
                    response = client.get(url)
                    response.raise_for_status()
                    if response.json()["counts"]["in_progress"]:
                        (destination / "interrupted.json").write_text(
                            json.dumps(response.json(), indent=2)
                        )
                        subprocess.run(
                            [
                                "docker",
                                "compose",
                                "kill",
                                "-s",
                                "SIGKILL",
                                "taskmanager",
                            ],
                            check=True,
                        )
                        return
                    time.sleep(0.05)
        finally:
            subprocess.run(
                ["docker", "compose", "unpause", "taskmanager"], capture_output=True
            )
            subprocess.run(["docker", "compose", "up", "-d", "taskmanager"], check=True)
        raise AssertionError("no active checkpoint observed; no failure was injected")

    started = time.monotonic()
    try:
        result = pipeline(fault=interrupt_checkpoint)
        after = httpx.get(url, timeout=10).json()
        (destination / "after.json").write_text(json.dumps(after, indent=2))
        assert after["counts"]["restored"] > before["counts"]["restored"]
        # A second restart must not reopen the now-final window for unseen old IDs.
        subprocess.run(["docker", "compose", "restart", "taskmanager"], check=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            recovered = httpx.get(url, timeout=10).json()
            if recovered["counts"]["restored"] > after["counts"]["restored"]:
                break
            time.sleep(1)
        else:
            raise AssertionError("second TaskManager recovery timed out")
        from .fixtures import realtime
        from .identity import snapshot_id
        from .ingestion import Publisher
        from .models import RawSnapshot
        from .lake import catalog
        from .serving import latest
        agency = result["agency"]
        feed = agency + "-late-after-restore"
        body = realtime(result["metric"]["window_start"] // 1000)
        snapshot = RawSnapshot(snapshot_id(feed_id=feed, body=body), agency, feed,
            time.time_ns() // 1_000_000, "fixture://late-after-restore", 200, None, None, None, body)
        Publisher(feed).publish(snapshot)
        while time.monotonic() < deadline:
            audits = catalog().load_table("gtfs.rejected_events").scan().to_arrow().to_pylist()
            rejected = set()
            for row in audits:
                audit = json.loads(row["record_json"])
                if audit.get("reason") == "beyond_retention" and audit.get("event", {}).get("feed_id") == feed:
                    rejected.add(audit["event"]["event_id"])
            if len(rejected) == 4:
                break
            time.sleep(2)
        assert len(rejected) == 4, "unseen late events were not audited after restore"
        visible = [row for row in latest() if row["agency_id"] == agency and row["window_start"] == result["metric"]["window_start"]]
        assert visible == [result["metric"]], "restart changed a finalized window"
        result["late_after_restore_rejected"] = len(rejected)
        result.update(
            parity=True, elapsed_seconds=time.monotonic() - started, job_id=job_id
        )
        (destination / "report.json").write_text(json.dumps(result, indent=2))
        return result
    finally:
        logs = subprocess.run(
            ["docker", "compose", "logs", "--tail=500", "taskmanager"],
            text=True,
            capture_output=True,
        )
        (destination / "taskmanager.log").write_text(logs.stdout + logs.stderr)
