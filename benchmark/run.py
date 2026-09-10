"""Repeat the real local smoke workload and retain evidence; no throughput claims."""

import argparse
import json
import platform
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.runs < 3:
        parser.error("at least three measured runs are required")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        parser.error(
            "commit local changes first so the recorded revision is reproducible"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "docker": json.loads(
            subprocess.check_output(
                ["docker", "info", "--format", "{{json .}}"], text=True
            )
        )["ServerVersion"],
        "docker_cpus": subprocess.check_output(
            ["docker", "info", "--format", "{{.NCPU}}"], text=True
        ).strip(),
        "docker_memory_bytes": subprocess.check_output(
            ["docker", "info", "--format", "{{.MemTotal}}"], text=True
        ).strip(),
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "scope": "fixture end-to-end correctness and completion time; not a throughput benchmark",
        "compose": subprocess.check_output(["docker", "compose", "config"], text=True),
        "images": subprocess.check_output(
            ["docker", "compose", "images", "--format", "json"], text=True
        ),
    }
    image_ids = subprocess.check_output(
        ["docker", "compose", "images", "-q"], text=True
    ).split()
    metadata["image_digests"] = [
        json.loads(
            subprocess.check_output(
                [
                    "docker",
                    "image",
                    "inspect",
                    image,
                    "--format",
                    "{{json .RepoDigests}}",
                ],
                text=True,
            )
        )
        for image in image_ids
    ]
    for tool in ("java", "mvn"):
        version = subprocess.run(
            [tool, "-version"], text=True, capture_output=True, check=True
        )
        metadata[tool] = version.stdout + version.stderr
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2))
    warmup = subprocess.run(["make", "smoke"], text=True, capture_output=True)
    (args.output / "warmup.json").write_text(
        json.dumps(
            {
                "returncode": warmup.returncode,
                "stdout": warmup.stdout,
                "stderr": warmup.stderr,
            },
            indent=2,
        )
    )
    warmup.check_returncode()
    durations = []
    for number in range(args.runs):
        start = time.monotonic()
        result = subprocess.run(["make", "smoke"], text=True, capture_output=True)
        duration = time.monotonic() - start
        (args.output / f"run-{number}.json").write_text(
            json.dumps(
                {
                    "duration_seconds": duration,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
                indent=2,
            )
        )
        result.check_returncode()
        durations.append(duration)
    report = {
        "runs": len(durations),
        "parity": True,
        "median_smoke_seconds": statistics.median(durations),
        "stdev_smoke_seconds": statistics.stdev(durations),
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
