
import asyncio
import inspect
import time
import uuid

from datetime import datetime, timezone

from market_data.logging_config import get_pipeline_logger
from market_data.database.run_history import save_pipeline_run
from market_data.database.loader import refresh_market_features_to_gold
from market_data.pipelines.twelvedata.main import run_twelvedata_pipeline
from market_data.pipelines.marketaux.main import run_marketaux_pipeline
from market_data.pipelines.execution_lock import pipeline_execution_lock
from market_data.storage.factory import get_storage
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("market_data.main")

KNOWN_STATUSES = {
    "success",
    "failed",
    "partial_failure",
    "skipped",
}


def _get_stage_status(stage_result) -> str:
    if stage_result is None:
        return "skipped"

    if isinstance(stage_result, list):
        if not stage_result:
            return "skipped"

        successful = 0
        failed = 0
        skipped = 0

        for item in stage_result:
            if not isinstance(item, dict):
                failed += 1
                continue

            if item.get("skipped") is True:
                skipped += 1
                continue

            if item.get("success") is True:
                successful += 1
                continue

            if item.get("success") is False:
                failed += 1
                continue

            status = item.get("status")

            if status == "success":
                successful += 1
            elif status == "skipped":
                skipped += 1
            elif status in ("failed", "partial_failure", "partial"):
                failed += 1
            else:
                failed += 1

        if failed and (successful or skipped):
            return "partial_failure"

        if failed:
            return "failed"

        if successful:
            return "success"

        return "skipped"

    if isinstance(stage_result, dict):
        status = stage_result.get("status")

        if status == "partial":
            return "partial_failure"

        if status in KNOWN_STATUSES:
            return status

        if stage_result.get("success") is True:
            return "success"

        if stage_result.get("success") is False:
            return "failed"

        return "failed"

    return "failed"


def _get_provider_status(result: dict) -> tuple[str, dict]:
    stages = {
        stage: _get_stage_status(stage_result)
        for stage, stage_result in result.items()
        if stage in ("bronze", "silver", "gold")
    }

    stage_errors = result.get("stage_errors", {})
    has_stage_errors = (
        isinstance(stage_errors, dict) and bool(stage_errors)
    )
    daily_limit_reached = result.get("daily_limit_reached") is True

    has_ingestion_errors = has_stage_errors or daily_limit_reached

    if not stages:
        if has_ingestion_errors:
            return "failed", stages

        status = result.get("status", "unknown")

        if status == "partial":
            status = "partial_failure"

        if status in KNOWN_STATUSES:
            return status, stages

        if result.get("success") is True:
            return "success", stages

        return "failed", stages

    statuses = list(stages.values())

    failures = sum(
        status in ("failed", "partial_failure")
        for status in statuses
    )
    successes = sum(status == "success" for status in statuses)

    if failures:
        if successes or any(
            status == "partial_failure"
            for status in statuses
        ):
            return "partial_failure", stages

        return "failed", stages

    if has_ingestion_errors:
        if successes:
            return "partial_failure", stages

        return "failed", stages

    if successes:
        return "success", stages

    if all(status == "skipped" for status in statuses):
        return "skipped", stages

    return "failed", stages

def _get_overall_status(results: dict) -> str:
    statuses = [
        results["twelvedata"]["status"],
        results["marketaux"]["status"],
        results["combined_gold"]["status"],
    ]

    failed = statuses.count("failed")
    partial = statuses.count("partial_failure")
    successful = statuses.count("success")
    skipped = statuses.count("skipped")

    if failed:
        if successful or partial or skipped:
            return "partial_failure"

        return "failed"

    if partial:
        return "partial_failure"

    if successful:
        return "success"

    if skipped == len(statuses):
        return "skipped"

    return "failed"


def _accepts_storage_argument(pipeline_function) -> bool:
    try:
        parameters = inspect.signature(
            pipeline_function
        ).parameters
    except (TypeError, ValueError):
        return True

    return (
        "storage" in parameters
        or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
    )


async def _run_provider(
    name: str,
    pipeline_function,
    storage: StorageBackend | None = None,
) -> dict:
    logger.info("%s pipeline started.", name)

    try:
        if (
            storage is not None
            and _accepts_storage_argument(pipeline_function)
        ):
            result = await asyncio.to_thread(
                pipeline_function,
                storage=storage,
            )
        else:
            result = await asyncio.to_thread(pipeline_function)

        if not isinstance(result, dict):
            raise TypeError(
                f"{name} pipeline returned an unexpected result: "
                f"{type(result).__name__}"
            )

        status, stages = _get_provider_status(result)

        logger.info(
            "%s pipeline completed with status: %s",
            name,
            status,
        )

        return {
            "status": status,
            "stages": stages,
            "result": result,
            "error": None,
        }

    except Exception as exc:
        logger.exception("%s pipeline failed.", name)

        return {
            "status": "failed",
            "stages": {},
            "result": None,
            "error": str(exc),
        }


async def _refresh_combined_gold(
    storage: StorageBackend | None = None,
) -> dict:
    logger.info(
        "========== COMBINED GOLD REFRESH STARTED =========="
    )

    try:
        if (
            storage is not None
            and _accepts_storage_argument(
                refresh_market_features_to_gold
            )
        ):
            rows = await asyncio.to_thread(
                refresh_market_features_to_gold,
                storage=storage,
            )
        else:
            rows = await asyncio.to_thread(
                refresh_market_features_to_gold
            )

        logger.info(
            "Combined Gold refresh and export completed. Total rows: %s",
            rows,
        )

        return {
            "status": "success",
            "rows": rows,
            "error": None,
        }

    except Exception as exc:
        logger.exception("Combined Gold refresh failed.")

        return {
            "status": "failed",
            "rows": 0,
            "error": str(exc),
        }


async def _run_market_data_pipeline_unlocked_async() -> dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    start_time = time.perf_counter()

    logger.info(
        "========== MARKET DATA PIPELINE STARTED | RUN %s ==========",
        run_id,
    )

    try:
        storage = get_storage()
    except Exception:
        logger.exception("Unable to initialise storage backend.")

        return {
            "run_id": run_id,
            "status": "failed",
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(
                time.perf_counter() - start_time,
                3,
            ),
            "storage_error": "Unable to initialise storage backend.",
            "twelvedata": {
                "status": "skipped",
                "stages": {},
                "result": None,
                "error": None,
            },
            "marketaux": {
                "status": "skipped",
                "stages": {},
                "result": None,
                "error": None,
            },
            "combined_gold": {
                "status": "skipped",
                "rows": 0,
                "error": None,
            },
        }

    twelvedata_task = _run_provider(
        "TwelveData",
        run_twelvedata_pipeline,
        storage,
    )

    marketaux_task = _run_provider(
        "Marketaux",
        run_marketaux_pipeline,
        storage,
    )

    twelvedata_result, marketaux_result = await asyncio.gather(
        twelvedata_task,
        marketaux_task,
    )

    combined_gold_result = await _refresh_combined_gold(storage)

    completed_at = datetime.now(timezone.utc)
    duration_seconds = time.perf_counter() - start_time

    results = {
        "run_id": run_id,
        "status": None,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round(duration_seconds, 3),
        "twelvedata": twelvedata_result,
        "marketaux": marketaux_result,
        "combined_gold": combined_gold_result,
    }

    results["status"] = _get_overall_status(results)

    logger.info(
        "Pipeline summary | Run: %s | Overall: %s | "
        "TwelveData: %s | Marketaux: %s | Combined Gold: %s | "
        "Duration: %.3f seconds",
        run_id,
        results["status"],
        twelvedata_result["status"],
        marketaux_result["status"],
        combined_gold_result["status"],
        duration_seconds,
    )

    try:
        await asyncio.to_thread(save_pipeline_run, results)

        logger.info(
            "Pipeline run history saved. Run: %s",
            run_id,
        )

    except Exception:
        logger.exception(
            "Could not save pipeline run history. Run: %s",
            run_id,
        )

    logger.info(
        "========== MARKET DATA PIPELINE COMPLETED | RUN %s ==========",
        run_id,
    )

    return results


async def run_market_data_pipeline_async() -> dict:
    with pipeline_execution_lock() as acquired:
        if not acquired:
            logger.warning(
                "Pipeline execution skipped: another execution is active."
            )

            timestamp = datetime.now(timezone.utc).isoformat()

            return {
                "run_id": None,
                "status": "skipped",
                "started_at": timestamp,
                "completed_at": timestamp,
                "duration_seconds": 0.0,
                "skip_reason": (
                    "Another market-data pipeline execution is active."
                ),
                "twelvedata": {
                    "status": "skipped",
                    "stages": {},
                    "result": None,
                    "error": None,
                },
                "marketaux": {
                    "status": "skipped",
                    "stages": {},
                    "result": None,
                    "error": None,
                },
                "combined_gold": {
                    "status": "skipped",
                    "rows": 0,
                    "error": None,
                },
            }

        return await _run_market_data_pipeline_unlocked_async()


def run_market_data_pipeline() -> dict:
    return asyncio.run(run_market_data_pipeline_async())
