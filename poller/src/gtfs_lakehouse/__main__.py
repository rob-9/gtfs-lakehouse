"""Local development commands for ingestion and the lakehouse."""

import argparse
import asyncio
import fcntl
import json
import signal
import tomllib
from pathlib import Path


def main():
    # macOS shells commonly allow only 256 descriptors, below Arrow's scan needs.
    import resource

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 4096 if hard == resource.RLIM_INFINITY else min(4096, hard)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    parser = argparse.ArgumentParser(prog="python -m gtfs_lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")
    commands.add_parser("smoke-platform")
    commands.add_parser("smoke")
    commands.add_parser("monitor")
    commands.add_parser("submit")
    commands.add_parser("redeploy")
    commands.add_parser("up")
    commands.add_parser("down")
    commands.add_parser("load-fixture-schedule")
    failure = commands.add_parser("test-recovery")
    failure.add_argument("--output", required=True)
    schedule = commands.add_parser("load-schedule")
    schedule.add_argument("archive")
    schedule.add_argument("--agency", required=True)
    schedule.add_argument("--effective-from", required=True)
    serving = commands.add_parser("serve")
    serving.add_argument("--once", action="store_true")
    queries = commands.add_parser("query")
    queries.add_argument("--generation", default="live-v2")
    pin = commands.add_parser("pin")
    pin.add_argument("path")
    replay = commands.add_parser("replay")
    replay.add_argument("manifest")
    replay.add_argument("--generation", required=True)
    coverage_pin = commands.add_parser("pin-coverage")
    coverage_pin.add_argument("path")
    coverage = commands.add_parser("coverage")
    coverage.add_argument("manifest")
    coverage.add_argument("--agency", required=True)
    coverage.add_argument("--schedule-version", required=True)
    coverage.add_argument("--service-date", required=True)
    coverage.add_argument("--as-of", required=True)
    coverage.add_argument("--grace-seconds", type=int, default=300)
    coverage.add_argument("--output")
    migration = commands.add_parser("migrate-inputs")
    migration.add_argument("manifest")
    migration.add_argument("--apply", action="store_true")
    fixture = commands.add_parser("fixtures")
    fixture.add_argument("--port", type=int, default=8090)
    fixture.add_argument("--host", default="127.0.0.1")
    poll = commands.add_parser("poll")
    poll.add_argument("--config", default="config/feeds.toml")
    poll.add_argument("--state-dir", default="var/poller")
    poll.add_argument("--once", action="store_true")
    poll.add_argument(
        "--metrics-port", type=int, help="Expose Prometheus polling metrics"
    )
    poll.add_argument("--metrics-host", default="127.0.0.1")
    args = parser.parse_args()
    if args.command == "fixtures":
        from .fixtures import serve

        serve(args.port, args.host)
    elif args.command in ("up", "down"):
        from .lifecycle import platform

        platform(args.command)
    elif args.command == "test-recovery":
        from .failures import recover_taskmanager

        print(json.dumps(recover_taskmanager(args.output), indent=2))
    elif args.command == "load-schedule":
        from .lake import load_schedule, publish_schedule
        import httpx

        if args.archive.startswith(("https://", "http://")):
            body = bytearray()
            with httpx.stream("GET", args.archive, timeout=30) as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > 16 * 1024 * 1024:
                        raise ValueError("archive exceeds local download limit")
        else:
            if Path(args.archive).stat().st_size > 16 * 1024 * 1024:
                raise ValueError("archive exceeds local profile limit")
            body = Path(args.archive).read_bytes()
        manifest = load_schedule(bytes(body), args.agency, args.effective_from)
        publish_schedule(manifest)
        print(
            json.dumps(
                {
                    "schedule_version": manifest["schedule_version"],
                    "tables": manifest["tables"],
                },
                indent=2,
            )
        )
    elif args.command in ("submit", "redeploy"):
        from .lifecycle import submit

        submit(upgrade=args.command == "redeploy")
    elif args.command == "monitor":
        from .monitoring import monitor

        monitor()
    elif args.command == "pin":
        from .replay import pin

        print(json.dumps(pin(args.path), indent=2))
    elif args.command == "replay":
        from .replay import rebuild

        print(
            json.dumps(
                rebuild(json.loads(Path(args.manifest).read_text()), args.generation),
                indent=2,
            )
        )
    elif args.command == "pin-coverage":
        from .coverage import pin_coverage

        print(json.dumps(pin_coverage(args.path), indent=2))
    elif args.command == "coverage":
        from .coverage import report, write_report

        result = report(
            json.loads(Path(args.manifest).read_text()),
            args.agency,
            args.schedule_version,
            args.service_date,
            args.as_of,
            args.grace_seconds,
        )
        if args.output:
            write_report(args.output, result)
        print(json.dumps(result, indent=2))
    elif args.command == "migrate-inputs":
        from .replay import migrate_inputs

        print(
            json.dumps(
                migrate_inputs(json.loads(Path(args.manifest).read_text()), args.apply),
                indent=2,
            )
        )
    elif args.command == "serve":
        from .serving import serve

        serve(args.once)
    elif args.command == "query":
        from .serving import latest

        print(json.dumps(latest(args.generation), indent=2))
    elif args.command == "load-fixture-schedule":
        from .fixtures import static_zip
        from .lake import load_schedule, publish_schedule

        manifest = load_schedule(static_zip(), "demo", "2020-01-01T00:00:00+00:00")
        publish_schedule(manifest)
        print(
            json.dumps(
                {
                    "schedule_version": manifest["schedule_version"],
                    "tables": manifest["tables"],
                },
                sort_keys=True,
            )
        )
    elif args.command == "bootstrap":
        from .services import bootstrap

        bootstrap()
    elif args.command == "smoke-platform":
        from .smoke import platform

        platform()
    elif args.command == "smoke":
        from .smoke import pipeline

        pipeline()
    elif args.command == "poll":
        from .ingestion import poll_source

        entries = tomllib.loads(Path(args.config).read_text())["feeds"]
        keys = [(entry["agency_id"], entry["feed_id"]) for entry in entries]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate agency/feed configuration")
        if not 1 <= len(entries) <= 8:
            raise ValueError("local profile supports 1–8 concurrent feeds")
        Path(args.state_dir).mkdir(parents=True, exist_ok=True)
        with open(Path(args.state_dir) / "owner.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            metrics = None
            if args.metrics_port is not None:
                from prometheus_client import start_http_server
                from .poller_metrics import PollerMetrics

                metrics = PollerMetrics()
                start_http_server(args.metrics_port, addr=args.metrics_host)

            async def run():
                stop = asyncio.Event()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    asyncio.get_running_loop().add_signal_handler(sig, stop.set)
                await asyncio.gather(
                    *(
                        poll_source(entry, args.state_dir, stop, args.once, metrics)
                        for entry in entries
                    )
                )

            asyncio.run(run())


if __name__ == "__main__":
    main()
