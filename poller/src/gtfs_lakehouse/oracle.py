"""Independent bounded aggregation for final-window parity checks."""

import hashlib
import math
from collections import defaultdict

WINDOW = 300000


def aggregate(events, generation="live-v1"):
    groups = defaultdict(dict)
    for event in events:
        if event.get("unmatched_reason") is not None:
            continue
        key = (
            event["agency_id"],
            event["route_id"],
            event["direction_id"],
            event["service_date"],
            event["observed_at"] // WINDOW * WINDOW,
        )
        groups[key][event["event_id"]] = event
    results = []
    for key, unique in sorted(groups.items()):
        latest = {}
        vehicles = set()
        canceled = set()
        arrivals = {}
        ordered = sorted(
            unique.values(), key=lambda event: (event["observed_at"], event["event_id"])
        )
        for event in ordered:
            if event["vehicle_id"] is not None:
                vehicles.add(event["vehicle_id"])
            trip = event["trip_id"]
            payload = event["payload"]
            if (payload.get("trip") or {}).get("schedule_relationship") == "CANCELED":
                canceled.add(trip)
                latest = {key: value for key, value in latest.items() if key[0] != trip}
            else:
                canceled.discard(trip)
                for stop in (payload.get("trip_update") or {}).get(
                    "stop_time_updates", []
                ):
                    sample = (trip, stop["stop_id"], stop["stop_sequence"])
                    if stop["arrival_delay"] is not None:
                        latest[sample] = stop["arrival_delay"]
                    else:
                        latest.pop(sample, None)
            position = payload.get("vehicle_position") or {}
            if (
                position.get("current_status") == "STOPPED_AT"
                and position.get("stop_id") is not None
            ):
                sample = (position["stop_id"], trip)
                arrivals[sample] = min(
                    arrivals.get(sample, event["observed_at"]), event["observed_at"]
                )
        stops = defaultdict(list)
        for (stop, trip), timestamp in arrivals.items():
            stops[stop].append(timestamp)
        headways = [
            (b - a) / 1000
            for times in stops.values()
            for a, b in zip(sorted(times), sorted(times)[1:])
        ]
        delays = sorted(latest.values())
        results.append(
            dict(
                schema_version=1,
                generation=generation,
                agency_id=key[0],
                route_id=key[1],
                direction_id=key[2],
                service_date=key[3],
                window_start=key[4],
                window_end=key[4] + WINDOW,
                version=1,
                input_digest=hashlib.sha256(
                    "\n".join(sorted(unique)).encode()
                ).hexdigest(),
                vehicle_count=len(vehicles),
                prediction_count=len(delays),
                canceled_trip_count=len(canceled),
                arrival_count=len(arrivals),
                headway_count=len(headways),
                mean_delay_seconds=sum(delays) / len(delays) if delays else None,
                p50_delay_seconds=delays[math.ceil(len(delays) * 0.5) - 1]
                if delays
                else None,
                p95_delay_seconds=delays[math.ceil(len(delays) * 0.95) - 1]
                if delays
                else None,
                adherence_rate=sum(-60 <= delay <= 300 for delay in delays)
                / len(delays)
                if delays
                else None,
                mean_headway_seconds=sum(headways) / len(headways)
                if headways
                else None,
            )
        )
    return results
