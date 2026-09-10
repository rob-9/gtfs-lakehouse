import json
from unittest.mock import Mock

import pytest

from gtfs_lakehouse import lifecycle


def test_unclean_shutdown_chooses_latest_completed_checkpoint(tmp_path, monkeypatch):
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps({"job_id": "job", "stopped": False, "savepoint": "s3://old"})
    )
    monkeypatch.setattr(lifecycle, "STATE", state)
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "flink/job/chk-9/_metadata"},
                {"Key": "flink/job/chk-10/_metadata"},
                {"Key": "flink/job/chk-11/pending-data"},
            ]
        }
    ]
    monkeypatch.setattr("gtfs_lakehouse.services.s3", lambda: client)
    assert lifecycle.restore_point() == "s3://checkpoints/flink/job/chk-10/_metadata"


def test_retained_history_without_restore_point_blocks_fresh_submission(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(lifecycle, "STATE", tmp_path / "missing.json")
    monkeypatch.setattr(lifecycle, "active_jobs", lambda: [])
    client = Mock()
    monkeypatch.setattr("gtfs_lakehouse.lake.catalog", lambda: client)
    run = Mock()
    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="retained history"):
        lifecycle.submit()
    run.assert_not_called()
