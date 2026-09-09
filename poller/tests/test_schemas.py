"""Exercise the actual wire schema against normalized protobuf fixtures."""

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import avro.io
import avro.schema
import pytest

from gtfs_lakehouse.normalize import normalize_feed
from test_normalize import _fixture

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


@pytest.mark.parametrize("name", ["gtfs_event", "route_metric"])
def test_schema_parses(name):
    assert avro.schema.parse((SCHEMAS / f"{name}.avsc").read_text()).type == "record"


def test_normalized_entities_round_trip_through_avro():
    schema = avro.schema.parse((SCHEMAS / "gtfs_event.avsc").read_text())
    events, failures = normalize_feed(
        _fixture(), agency_id="demo", feed_id="vehicles", ingested_at=1_700_000_010_000,
    )
    assert not failures
    for event in events:
        record = event.to_dict()
        # Apache Avro represents timestamp-millis as aware datetime objects.
        for field in ("observed_at", "feed_generated_at", "ingested_at"):
            if record[field] is not None:
                record[field] = datetime.fromtimestamp(record[field] / 1000, timezone.utc)
        buffer = BytesIO()
        avro.io.DatumWriter(schema).write(record, avro.io.BinaryEncoder(buffer))
        buffer.seek(0)
        restored = avro.io.DatumReader(schema).read(avro.io.BinaryDecoder(buffer))
        assert restored == record
