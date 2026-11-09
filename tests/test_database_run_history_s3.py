
from datetime import datetime, timezone

import pytest

from market_data.database import run_history


class FakeStorage:
    def __init__(self):
        self.records = {}

    def write_json(self, key, data, overwrite=True):
        if not overwrite and key in self.records:
            raise FileExistsError(key)

        self.records[key] = data
        return key

    def read_json(self, key):
        return self.records[key]

    def exists(self, key):
        return key in self.records

    def list_files(self, prefix="", pattern="*"):
        return sorted(
            key
            for key in self.records
            if key.startswith(prefix + "/")
            and key.endswith(".json")
        )


def make_results(run_id, minute=0):
    started_at = datetime(
        2026, 11, 8, 0, minute, tzinfo=timezone.utc
    )
    completed_at = datetime(
        2026, 11, 8, 0, minute + 1, tzinfo=timezone.utc
    )

    return {
        "run_id": run_id,
        "status": "partial_failure",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 60.0,
        "twelvedata": {
            "status": "success",
            "stages": {"bronze": "success"},
            "error": None,
        },
        "marketaux": {
            "status": "failed",
            "stages": {},
            "error": "API unavailable",
        },
        "combined_gold": {
            "status": "success",
            "error": None,
        },
    }


@pytest.fixture
def s3_storage(monkeypatch):
    storage = FakeStorage()

    monkeypatch.setenv("MARKET_DATA_STORAGE_BACKEND", "s3")

    monkeypatch.setattr(
        "market_data.storage.factory.get_storage",
        lambda: storage,
    )

    return storage


def test_save_pipeline_run_to_s3(s3_storage):
    results = make_results("run-001")

    assert run_history.save_pipeline_run(results) == "run-001"

    record = s3_storage.records[
        "state/pipeline_runs/run-001.json"
    ]

    assert record["run_id"] == "run-001"
    assert record["status"] == "partial_failure"
    assert record["error_summary"] == "marketaux: API unavailable"


def test_save_pipeline_run_is_idempotent_in_s3(s3_storage):
    results = make_results("run-001")

    run_history.save_pipeline_run(results)

    updated = {
        **results,
        "status": "success",
        "duration_seconds": 75.0,
    }

    run_history.save_pipeline_run(updated)

    assert len(s3_storage.records) == 1
    assert (
        s3_storage.records[
            "state/pipeline_runs/run-001.json"
        ]["duration_seconds"]
        == 75.0
    )


def test_get_pipeline_run_from_s3(s3_storage):
    results = make_results("run-001")
    run_history.save_pipeline_run(results)

    record = run_history.get_pipeline_run("run-001")

    assert record is not None
    assert record["run_id"] == "run-001"
    assert record["results"]["marketaux"]["error"] == "API unavailable"


def test_get_pipeline_run_returns_none_for_unknown_s3_id(s3_storage):
    assert run_history.get_pipeline_run("unknown") is None


def test_get_recent_pipeline_runs_from_s3(s3_storage):
    run_history.save_pipeline_run(make_results("run-001", 0))
    run_history.save_pipeline_run(make_results("run-002", 5))
    run_history.save_pipeline_run(make_results("run-003", 10))

    records = run_history.get_recent_pipeline_runs()

    assert [record["run_id"] for record in records] == [
        "run-003",
        "run-002",
        "run-001",
    ]


def test_get_recent_pipeline_runs_respects_limit_in_s3(s3_storage):
    run_history.save_pipeline_run(make_results("run-001", 0))
    run_history.save_pipeline_run(make_results("run-002", 5))
    run_history.save_pipeline_run(make_results("run-003", 10))

    records = run_history.get_recent_pipeline_runs(limit=2)

    assert [record["run_id"] for record in records] == [
        "run-003",
        "run-002",
    ]


def test_get_recent_pipeline_runs_empty_s3(s3_storage):
    assert run_history.get_recent_pipeline_runs() == []


def test_get_recent_pipeline_runs_rejects_invalid_limit_s3(
    s3_storage,
):
    with pytest.raises(ValueError):
        run_history.get_recent_pipeline_runs(limit=0)


def test_s3_run_id_is_encoded(s3_storage):
    results = make_results("run/with spaces")

    run_history.save_pipeline_run(results)

    assert (
        "state/pipeline_runs/run%2Fwith%20spaces.json"
        in s3_storage.records
    )
