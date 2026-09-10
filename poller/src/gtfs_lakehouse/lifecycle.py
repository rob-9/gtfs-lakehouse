"""Local Flink submission and state-preserving upgrades."""

import json
import re
import subprocess
from pathlib import Path

import httpx


STATE = Path("var/flink-state.json")
FLINK = ["docker", "compose", "exec", "-T", "jobmanager", "flink"]


def active_jobs(allow_offline=False):
    try:
        response = httpx.get("http://localhost:8081/jobs/overview", timeout=10)
        response.raise_for_status()
    except httpx.ConnectError:
        if allow_offline:
            return []
        raise
    jobs = [
        job
        for job in response.json()["jobs"]
        if job["state"] not in ("FINISHED", "CANCELED", "FAILED")
    ]
    if len(jobs) > 1:
        raise RuntimeError("multiple active jobs; select the intended job manually")
    return jobs


def remember(record):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    pending = STATE.with_suffix(".tmp")
    pending.write_text(json.dumps(record, indent=2))
    pending.replace(STATE)


def stop(job):
    stopped = subprocess.run(
        FLINK + ["stop", "--savepointPath", "s3://checkpoints/savepoints/", job["jid"]],
        check=False,
        capture_output=True,
        text=True,
    )
    if stopped.returncode:
        print(stopped.stdout + stopped.stderr, flush=True)
        stopped.check_returncode()
    match = re.search(r"(s3://checkpoints/savepoints/savepoint-[^\s]+)", stopped.stdout)
    if not match:
        raise RuntimeError(
            "job stopped but savepoint path was not parsed; inspect Flink logs before resubmitting"
        )
    record = {"savepoint": match.group(1), "job_id": job["jid"], "stopped": True}
    remember(record)
    print(f"Saved {record['savepoint']}", flush=True)


def restore_point():
    if not STATE.exists():
        return None
    record = json.loads(STATE.read_text())
    if not record.get("stopped"):
        from .services import s3

        prefix = f"flink/{record['job_id']}/"
        checkpoints = []
        for page in (
            s3()
            .get_paginator("list_objects_v2")
            .paginate(Bucket="checkpoints", Prefix=prefix)
        ):
            for item in page.get("Contents", []):
                match = re.fullmatch(
                    re.escape(prefix) + r"chk-(\d+)/_metadata", item["Key"]
                )
                if match:
                    checkpoints.append((int(match.group(1)), item["Key"]))
        if checkpoints:
            return "s3://checkpoints/" + max(checkpoints)[1]
    return record.get("savepoint")


def submit(upgrade=False):
    jobs = active_jobs()
    if jobs and not upgrade:
        print(
            f"Pipeline already active: {jobs[0]['jid']}. Use make redeploy to preserve its state."
        )
        return
    if jobs:
        stop(jobs[0])
    point = restore_point()
    if point is None:
        from .lake import catalog
        from pyiceberg.exceptions import NoSuchTableError

        try:
            target = catalog().load_table("gtfs.normalized_events")
            if target.current_snapshot() is not None:
                raise RuntimeError(
                    "retained history has no tracked restore point; recover the checkpoint before submitting"
                )
        except NoSuchTableError:
            pass
    restore = ["-s", point] if point else []
    result = subprocess.run(
        FLINK + ["run", "-d"] + restore + ["/opt/jobs/lakehouse-job-0.1.0.jar"],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout, flush=True)
    match = re.search(r"JobID ([0-9a-f]{32})", result.stdout)
    if not match:
        raise RuntimeError(
            "submission returned no job ID; inspect Flink before retrying"
        )
    remember({"savepoint": point, "job_id": match.group(1), "stopped": False})


def platform(action):
    jobs = active_jobs(allow_offline=True)
    if jobs:
        stop(jobs[0])
    if action == "down":
        subprocess.run(
            [
                "docker",
                "compose",
                "--profile",
                "workers",
                "--profile",
                "observability",
                "down",
            ],
            check=True,
        )
    else:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180"],
            check=True,
        )
        from .services import bootstrap

        bootstrap()
        if jobs:
            submit()
