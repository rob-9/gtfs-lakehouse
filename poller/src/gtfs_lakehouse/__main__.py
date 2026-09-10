"""Local development commands for ingestion and the lakehouse."""

import argparse
import asyncio
import fcntl
import json
import signal
import tomllib
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(prog="python -m gtfs_lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")
    commands.add_parser("smoke-platform")
    commands.add_parser("load-fixture-schedule")
    fixture = commands.add_parser("fixtures")
    fixture.add_argument("--port", type=int, default=8090)
    poll = commands.add_parser("poll")
    poll.add_argument("--config", default="config/feeds.toml")
    poll.add_argument("--state-dir", default="var/poller")
    poll.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.command == "fixtures":
        from .fixtures import serve
        serve(args.port)
    elif args.command == "load-fixture-schedule":
        from .fixtures import static_zip
        from .lake import load_schedule, publish_schedule
        manifest = load_schedule(static_zip(), "demo", "2020-01-01T00:00:00+00:00")
        publish_schedule(manifest)
        print(json.dumps(manifest, sort_keys=True))
    elif args.command == "bootstrap":
        from .services import bootstrap
        bootstrap()
    elif args.command == "smoke-platform":
        from .smoke import platform
        platform()
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

            async def run():
                stop = asyncio.Event()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    asyncio.get_running_loop().add_signal_handler(sig, stop.set)
                await asyncio.gather(*(poll_source(entry, args.state_dir, stop, args.once) for entry in entries))

            asyncio.run(run())


if __name__ == "__main__":
    main()
