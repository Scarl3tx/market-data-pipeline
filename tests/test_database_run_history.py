
import json
from datetime import datetime, timezone

import pytest

from market_data.database.connection import get_database_connection
from market_data.database.run_history import (
    get_pipeline_run,
    get_recent_pipeline_runs,
    save_pipeline_run,
)
from market_data.database.tables import create_pipeline_runs_table


def setup_database(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    # These tests explicitly exercise the local DuckDB implementation.
    monkeypatch.setenv("MARKET_DATA_STORAGE_BACKEND", "local")

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(db_connection, "DATABASE_DIR", database_dir)
    monkeypatch.setattr(db_connection, "DATABASE_PATH", database_path)

    return get_database_connection()


def make_results(run_id="test-run-001", minute=0):
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
            "stages": {
                "bronze": "success",
                "silver": "success",
                "gold": "success",
            },
            "result": {"status": "success"},
            "error": None,
        },
        "marketaux": {
            "status": "failed",
            "stages": {},
            "result": None,
            "error": "API unavailable",
        },
        "combined_gold": {
            "status": "success",
            "rows": 100,
            "error": None,
        },
    }


def test_create_pipeline_runs_table(tmp_path, monkeypatch):
    connection = setup_database(tmp_path, monkeypatch)

    try:
        create_pipeline_runs_table(connection)
        create_pipeline_runs_table(connection)

        columns = connection.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'pipeline_runs'
            ORDER BY ordinal_position
        """).fetchall()

        assert [row[0] for row in columns] == [
            "run_id",
            "started_at",
            "completed_at",
            "duration_seconds",
            "status",
            "twelvedata_status",
            "marketaux_status",
            "combined_gold_status",
            "twelvedata_stages_json",
            "marketaux_stages_json",
            "error_summary",
            "results_json",
            "recorded_at",
        ]

    finally:
        connection.close()


def test_save_pipeline_run(tmp_path, monkeypatch):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    results = make_results()

    assert save_pipeline_run(results) == "test-run-001"

    connection = get_database_connection()

    try:
        row = connection.execute("""
            SELECT
                run_id,
                status,
                twelvedata_status,
                marketaux_status,
                combined_gold_status,
                error_summary,
                results_json
            FROM pipeline_runs
        """).fetchone()
        assert row is not None, "Expected pipeline run record was not found"
        assert row[:5] == (
            "test-run-001",
            "partial_failure",
            "success",
            "failed",
            "success",
        )

        assert row[5] == "marketaux: API unavailable"

        stored_results = json.loads(row[6])
        assert stored_results["run_id"] == "test-run-001"
        assert stored_results["marketaux"]["error"] == "API unavailable"

    finally:
        connection.close()


def test_save_pipeline_run_is_idempotent(tmp_path, monkeypatch):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    results = make_results()

    save_pipeline_run(results)

    updated_results = {
        **results,
        "status": "success",
        "duration_seconds": 75.0,
        "marketaux": {
            **results["marketaux"],
            "status": "success",
            "error": None,
        },
    }

    save_pipeline_run(updated_results)

    connection = get_database_connection()

    try:
        rows = connection.execute("""
            SELECT run_id, status, duration_seconds
            FROM pipeline_runs
        """).fetchall()

        assert rows == [
            ("test-run-001", "success", 75.0)
        ]

    finally:
        connection.close()


def test_save_pipeline_run_preserves_empty_error_summary(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    results = make_results()

    results["twelvedata"]["error"] = None
    results["marketaux"]["error"] = None
    results["combined_gold"]["error"] = None

    save_pipeline_run(results)

    connection = get_database_connection()

    try:
        row = connection.execute("""
            SELECT error_summary
            FROM pipeline_runs
        """).fetchone()

        assert row is not None, "Expected pipeline run record was not found"

        error_summary = row[0]
        assert error_summary in (None, "")
    finally:
        connection.close()


def test_save_pipeline_run_rejects_missing_required_fields(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    with pytest.raises(KeyError):
        save_pipeline_run({"run_id": "incomplete"})


def test_get_pipeline_run_returns_parsed_result(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    results = make_results()
    save_pipeline_run(results)

    run = get_pipeline_run("test-run-001")

    assert run is not None
    assert run["run_id"] == "test-run-001"
    assert run["status"] == "partial_failure"
    assert run["twelvedata_status"] == "success"
    assert run["marketaux_status"] == "failed"
    assert run["combined_gold_status"] == "success"

    assert run["twelvedata_stages"] == {
        "bronze": "success",
        "silver": "success",
        "gold": "success",
    }
    assert run["marketaux_stages"] == {}

    assert isinstance(run["results"], dict)
    assert run["results"]["marketaux"]["error"] == "API unavailable"
    assert run["error_summary"] == "marketaux: API unavailable"


def test_get_pipeline_run_returns_none_for_unknown_id(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    assert get_pipeline_run("unknown-run") is None


def test_get_pipeline_run_rejects_empty_id():
    with pytest.raises(ValueError):
        get_pipeline_run("")

    with pytest.raises(ValueError):
        get_pipeline_run("   ")


def test_get_recent_pipeline_runs_returns_newest_first(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    save_pipeline_run(make_results("run-001", minute=0))
    save_pipeline_run(make_results("run-002", minute=5))
    save_pipeline_run(make_results("run-003", minute=10))

    runs = get_recent_pipeline_runs()

    assert [run["run_id"] for run in runs] == [
        "run-003",
        "run-002",
        "run-001",
    ]


def test_get_recent_pipeline_runs_respects_limit(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    save_pipeline_run(make_results("run-001", minute=0))
    save_pipeline_run(make_results("run-002", minute=5))
    save_pipeline_run(make_results("run-003", minute=10))

    runs = get_recent_pipeline_runs(limit=2)

    assert [run["run_id"] for run in runs] == [
        "run-003",
        "run-002",
    ]


def test_get_recent_pipeline_runs_returns_empty_list(
    tmp_path,
    monkeypatch,
):
    connection = setup_database(tmp_path, monkeypatch)
    connection.close()

    assert get_recent_pipeline_runs() == []


@pytest.mark.parametrize("limit", [0, -1, 1.5, "5", True])
def test_get_recent_pipeline_runs_rejects_invalid_limit(limit):
    with pytest.raises(ValueError):
        get_recent_pipeline_runs(limit=limit)
