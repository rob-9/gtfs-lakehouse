"""Validated static schedules, committed version manifests, and reference joins."""

import csv
import json
import re
from datetime import date, datetime, time, timedelta
from io import BytesIO, StringIO
from zoneinfo import ZoneInfo
from zipfile import ZipFile

from .identity import sha256_hex

REQUIRED = {
    "agency": {"agency_name", "agency_url", "agency_timezone"},
    "routes": {"route_id", "route_type"},
    "stops": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    "trips": {"trip_id", "route_id", "service_id"},
    "stop_times": {"trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"},
}


def seconds(value):
    if not re.fullmatch(r"\d{1,3}:\d{2}:\d{2}", value):
        raise ValueError(f"invalid GTFS time: {value}")
    hour, minute, second = map(int, value.split(":"))
    if minute > 59 or second > 59:
        raise ValueError(f"invalid GTFS time: {value}")
    return hour * 3600 + minute * 60 + second


def service_instant(day, value, timezone):
    # GTFS defines times relative to local noon minus twelve hours, including DST days.
    noon = datetime.combine(day, time(12), ZoneInfo(timezone))
    return int(noon.timestamp()) - 43200 + seconds(value)


def parse_archive(body):
    result = {}
    with ZipFile(BytesIO(body)) as archive:
        entries = archive.infolist()
        if len(entries) > 100 or sum(item.file_size for item in entries) > 64 * 1024 * 1024:
            raise ValueError("schedule archive exceeds local profile limits")
        names = [item.filename for item in entries]
        if len(names) != len(set(names)) or any("/" in name or "\\" in name for name in names):
            raise ValueError("archive must have unique root-level filenames")
        if "frequencies.txt" in names:
            raise ValueError("frequency-based schedules are not supported")
        for name in REQUIRED.keys() | {"calendar", "calendar_dates"}:
            if f"{name}.txt" not in names:
                if name in REQUIRED:
                    raise ValueError(f"missing {name}.txt")
                continue
            reader = csv.DictReader(StringIO(archive.read(f"{name}.txt").decode("utf-8-sig")))
            required = REQUIRED.get(name, {"service_id", "date", "exception_type"} if name == "calendar_dates"
                                    else {"service_id", "start_date", "end_date", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"})
            if not required <= set(reader.fieldnames or []):
                raise ValueError(f"missing columns in {name}")
            rows = list(reader)
            if any(None in row or any(v is None for v in row.values()) for row in rows):
                raise ValueError(f"malformed rows in {name}")
            result[name] = rows
    if not result.get("calendar") and not result.get("calendar_dates"):
        raise ValueError("a service calendar is required")
    if len(result["agency"]) != 1:
        raise ValueError("local profile requires one agency per archive")
    ZoneInfo(result["agency"][0]["agency_timezone"])
    keys = {}
    for table, field in [("routes", "route_id"), ("stops", "stop_id"), ("trips", "trip_id")]:
        values = [row[field] for row in result[table]]
        if any(not v for v in values) or len(set(values)) != len(values):
            raise ValueError(f"invalid or duplicate {field}")
        keys[table] = set(values)
    services = {row["service_id"] for table in ("calendar", "calendar_dates") for row in result.get(table, [])}
    for trip in result["trips"]:
        if trip["route_id"] not in keys["routes"] or trip["service_id"] not in services:
            raise ValueError("unresolved route or service")
    for stop in result["stops"]:
        if not -90 <= float(stop["stop_lat"]) <= 90 or not -180 <= float(stop["stop_lon"]) <= 180:
            raise ValueError("invalid stop coordinates")
    seen = set()
    for row in result["stop_times"]:
        key = (row["trip_id"], int(row["stop_sequence"]))
        if key in seen or key[1] < 0:
            raise ValueError("duplicate or invalid stop sequence")
        seen.add(key)
        if row["trip_id"] not in keys["trips"] or row["stop_id"] not in keys["stops"]:
            raise ValueError("unresolved stop or trip")
        for field in ("arrival_time", "departure_time"):
            seconds(row[field])
    return result


def active(schedule, service, day):
    compact = day.strftime("%Y%m%d")
    exceptions = [row for row in schedule.get("calendar_dates", []) if row["service_id"] == service and row["date"] == compact]
    if exceptions:
        return exceptions[-1]["exception_type"] == "1"
    weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][day.weekday()]
    return any(row["service_id"] == service and row["start_date"] <= compact <= row["end_date"]
               and row[weekday] == "1" for row in schedule.get("calendar", []))


def enrich(event, schedule, version):
    descriptor = event["payload"].get("trip") or {}
    trip = next((row for row in schedule["trips"] if row["trip_id"] == event["trip_id"]), None)
    result = dict(event, schedule_version=version)
    if trip is None:
        return result | {"unmatched_reason": "unknown_trip"}
    if not descriptor.get("start_date"):
        return result | {"unmatched_reason": "ambiguous_service_date"}
    day = datetime.strptime(descriptor["start_date"], "%Y%m%d").date()
    if not active(schedule, trip["service_id"], day):
        return result | {"unmatched_reason": "inactive_service"}
    return result | {"route_id": trip["route_id"], "direction_id": int(trip.get("direction_id") or -1),
                     "service_date": day.isoformat(), "unmatched_reason": None}
