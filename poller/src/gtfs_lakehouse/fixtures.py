"""Deterministic local GTFS fixtures and an HTTP source with validators."""

import hashlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile, ZipInfo

from google.transit import gtfs_realtime_pb2 as pb

ROOT = Path(__file__).resolve().parents[3]
EPOCH = int(datetime(2026, 9, 10, 15, 0, tzinfo=timezone.utc).timestamp())


def static_zip() -> bytes:
    result = BytesIO()
    with ZipFile(result, "w") as archive:
        for path in sorted((ROOT / "tests/fixtures/static").glob("*.txt")):
            archive.writestr(ZipInfo(path.name, date_time=(2020, 1, 1, 0, 0, 0)), path.read_bytes())
    return result.getvalue()


def realtime(timestamp: int = EPOCH) -> bytes:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp
    for number, delay in [(1, 60), (2, 120)]:
        entity = feed.entity.add(id=f"trip-{number}")
        update = entity.trip_update
        update.timestamp = timestamp
        update.trip.trip_id = f"T{number}"
        update.trip.route_id = "R1"
        update.trip.start_date = "20260910"
        update.trip.direction_id = 0
        update.vehicle.id = f"V{number}"
        stop = update.stop_time_update.add(stop_sequence=1, stop_id="S1")
        stop.arrival.delay = delay
    entity = feed.entity.add(id="trip-3")
    entity.trip_update.timestamp = timestamp
    entity.trip_update.trip.trip_id = "T3"
    entity.trip_update.trip.start_date = "20260910"
    entity.trip_update.trip.schedule_relationship = pb.TripDescriptor.CANCELED
    vehicle = feed.entity.add(id="vehicle-1").vehicle
    vehicle.timestamp = timestamp
    vehicle.trip.trip_id = "T1"
    vehicle.trip.start_date = "20260910"
    vehicle.vehicle.id = "V1"
    vehicle.position.latitude = 33.68
    vehicle.position.longitude = -117.82
    vehicle.stop_id = "S1"
    vehicle.current_stop_sequence = 1
    vehicle.current_status = pb.VehiclePosition.STOPPED_AT
    return feed.SerializeToString(deterministic=True)


def serve(port: int = 8090):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/feed.pb", "/static.zip"):
                self.send_error(404)
                return
            body = realtime() if self.path == "/feed.pb" else static_zip()
            etag = '"' + hashlib.sha256(body).hexdigest() + '"'
            if self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("ETag", etag)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            self.wfile.write(body)

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
