from gtfs_lakehouse.ingestion import Outbox
from gtfs_lakehouse.models import RawSnapshot
from gtfs_lakehouse.polling import PollState


def test_pending_snapshot_survives_restart_without_advancing_validators(tmp_path):
    path = tmp_path / "outbox.sqlite"
    snapshot = RawSnapshot("id", "a", "f", 123, "https://example.test", 200, None, "etag", None, b"raw")
    box = Outbox(path)
    box.stage(snapshot)
    box.db.close()
    recovered = Outbox(path)
    assert recovered.pending() == snapshot
    assert recovered.state() == PollState()
    recovered.acknowledge(PollState("etag"))
    recovered.db.close()
    committed = Outbox(path)
    assert committed.pending() is None
    assert committed.state() == PollState("etag")
    committed.db.close()
