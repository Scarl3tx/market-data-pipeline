
import json
import os
from datetime import datetime, timezone
from urllib.parse import quote

from market_data.database.connection import get_database_connection
from market_data.database.tables import create_pipeline_runs_table
from market_data.database.write_lock import DATABASE_WRITE_LOCK


S3_RUN_HISTORY_PREFIX = "state/pipeline_runs"


def _uses_s3() -> bool:
    return (
        os.getenv("MARKET_DATA_STORAGE_BACKEND", "local")
        .strip()
        .lower()
        == "s3"
    )


def _get_error_summary(results: dict) -> str | None:
    errors = []

    for provider in ("twelvedata", "marketaux", "combined_gold"):
        provider_result = results.get(provider, {})
        error = provider_result.get("error")

        if error:
            errors.append(f"{provider}: {error}")

    return " | ".join(errors) if errors else None


def _row_to_dict(row) -> dict:
    """Convert a database row into a Python dictionary."""

    return {
        "run_id": row[0],
        "started_at": row[1],
        "completed_at": row[2],
        "duration_seconds": row[3],
        "status": row[4],
        "twelvedata_status": row[5],
        "marketaux_status": row[6],
        "combined_gold_status": row[7],
        "twelvedata_stages": json.loads(row[8]),
        "marketaux_stages": json.loads(row[9]),
        "error_summary": row[10],
        "results": json.loads(row[11]),
        "recorded_at": row[12],
    }


def _build_s3_record(results: dict) -> dict:
    """Build a JSON-serializable pipeline run record."""

    return {
        "run_id": results["run_id"],
        "started_at": str(results["started_at"]),
        "completed_at": str(results["completed_at"]),
        "duration_seconds": results["duration_seconds"],
        "status": results["status"],
        "twelvedata_status": results["twelvedata"]["status"],
        "marketaux_status": results["marketaux"]["status"],
        "combined_gold_status": results["combined_gold"]["status"],
        "twelvedata_stages": results["twelvedata"].get("stages", {}),
        "marketaux_stages": results["marketaux"].get("stages", {}),
        "error_summary": _get_error_summary(results),
        "results": json.loads(json.dumps(results, default=str)),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def _s3_run_key(run_id: str) -> str:
    """Return a safe object key for a pipeline run."""

    encoded_run_id = quote(run_id, safe="")

    return f"{S3_RUN_HISTORY_PREFIX}/{encoded_run_id}.json"


def _save_pipeline_run_to_s3(results: dict) -> str:
    from market_data.storage.factory import get_storage

    record = _build_s3_record(results)
    storage = get_storage()

    storage.write_json(
        _s3_run_key(record["run_id"]),
        record,
        overwrite=True,
    )

    return record["run_id"]


def _get_pipeline_run_from_s3(run_id: str) -> dict | None:
    from market_data.storage.factory import get_storage

    storage = get_storage()
    key = _s3_run_key(run_id)

    if not storage.exists(key):
        return None

    return storage.read_json(key)


def _get_recent_pipeline_runs_from_s3(limit: int) -> list[dict]:
    from market_data.storage.factory import get_storage

    storage = get_storage()

    keys = storage.list_files(
        prefix=S3_RUN_HISTORY_PREFIX,
        pattern="*.json",
    )

    records = [
        storage.read_json(key)
        for key in keys
    ]

    records.sort(
        key=lambda record: (
            record.get("started_at", ""),
            record.get("recorded_at", ""),
        ),
        reverse=True,
    )

    return records[:limit]


def save_pipeline_run(results: dict) -> str:
    """Persist one completed pipeline execution."""

    if _uses_s3():
        return _save_pipeline_run_to_s3(results)

    run_id = results["run_id"]

    with DATABASE_WRITE_LOCK:
        connection = get_database_connection()

        try:
            create_pipeline_runs_table(connection)

            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id,
                    started_at,
                    completed_at,
                    duration_seconds,
                    status,
                    twelvedata_status,
                    marketaux_status,
                    combined_gold_status,
                    twelvedata_stages_json,
                    marketaux_stages_json,
                    error_summary,
                    results_json
                )
                VALUES (
                    ?,
                    CAST(? AS TIMESTAMPTZ),
                    CAST(? AS TIMESTAMPTZ),
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
                ON CONFLICT (run_id)
                DO UPDATE SET
                    completed_at = EXCLUDED.completed_at,
                    duration_seconds = EXCLUDED.duration_seconds,
                    status = EXCLUDED.status,
                    twelvedata_status = EXCLUDED.twelvedata_status,
                    marketaux_status = EXCLUDED.marketaux_status,
                    combined_gold_status = EXCLUDED.combined_gold_status,
                    twelvedata_stages_json = EXCLUDED.twelvedata_stages_json,
                    marketaux_stages_json = EXCLUDED.marketaux_stages_json,
                    error_summary = EXCLUDED.error_summary,
                    results_json = EXCLUDED.results_json
                """,
                [
                    run_id,
                    results["started_at"],
                    results["completed_at"],
                    results["duration_seconds"],
                    results["status"],
                    results["twelvedata"]["status"],
                    results["marketaux"]["status"],
                    results["combined_gold"]["status"],
                    json.dumps(
                        results["twelvedata"].get("stages", {}),
                        default=str,
                    ),
                    json.dumps(
                        results["marketaux"].get("stages", {}),
                        default=str,
                    ),
                    _get_error_summary(results),
                    json.dumps(results, default=str),
                ],
            )

            return run_id

        finally:
            connection.close()


def get_recent_pipeline_runs(limit: int = 10) -> list[dict]:
    """Return the most recent pipeline executions."""

    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer.")

    if _uses_s3():
        return _get_recent_pipeline_runs_from_s3(limit)

    with DATABASE_WRITE_LOCK:
        connection = get_database_connection()

        try:
            create_pipeline_runs_table(connection)

            rows = connection.execute(
                """
                SELECT
                    run_id,
                    CAST(started_at AS VARCHAR),
                    CAST(completed_at AS VARCHAR),
                    duration_seconds,
                    status,
                    twelvedata_status,
                    marketaux_status,
                    combined_gold_status,
                    twelvedata_stages_json,
                    marketaux_stages_json,
                    error_summary,
                    results_json,
                    CAST(recorded_at AS VARCHAR)
                FROM pipeline_runs
                ORDER BY started_at DESC, recorded_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()

            return [_row_to_dict(row) for row in rows]

        finally:
            connection.close()


def get_pipeline_run(run_id: str) -> dict | None:
    """Return one pipeline execution, or None if it does not exist."""

    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string.")

    if _uses_s3():
        return _get_pipeline_run_from_s3(run_id)

    with DATABASE_WRITE_LOCK:
        connection = get_database_connection()

        try:
            create_pipeline_runs_table(connection)

            row = connection.execute(
                """
                SELECT
                    run_id,
                    CAST(started_at AS VARCHAR),
                    CAST(completed_at AS VARCHAR),
                    duration_seconds,
                    status,
                    twelvedata_status,
                    marketaux_status,
                    combined_gold_status,
                    twelvedata_stages_json,
                    marketaux_stages_json,
                    error_summary,
                    results_json,
                    CAST(recorded_at AS VARCHAR)
                FROM pipeline_runs
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()

            return _row_to_dict(row) if row is not None else None

        finally:
            connection.close()
