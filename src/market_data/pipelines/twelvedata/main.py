
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)

from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.bronze.coordinator import (
    run_twelvedata_bronze,
)
from market_data.pipelines.twelvedata.silver.coordinator import (
    run_twelvedata_silver,
)
from market_data.pipelines.twelvedata.gold.coordinator import (
    run_twelvedata_gold,
)

from market_data.logging_config import get_pipeline_logger


logger = get_pipeline_logger("twelvedata.main")


def _run_stage(stage_function, storage: StorageBackend | None):
    """Run a stage while preserving the existing local call signature."""

    if storage is None:
        return stage_function()

    return stage_function(storage=storage)


def run_twelvedata_pipeline(
    storage: StorageBackend | None = None,
) -> dict:
    """Run all Twelve Data stages using the configured storage backend."""

    logger.info("========== TWELVE DATA PIPELINE STARTED ==========")

    bronze_results = []
    silver_results = []
    gold_results = []

    daily_limit_reached = False
    stage_errors = {}

    # 1. Bronze ingestion
    try:
        bronze_results = _run_stage(
            run_twelvedata_bronze,
            storage,
        )

    except TwelveDataDailyLimitError as exc:
        daily_limit_reached = True
        stage_errors["bronze"] = str(exc)

        logger.warning(
            "Twelve Data daily limit reached. "
            "Continuing with previously collected Bronze data. %s",
            exc,
        )

    except Exception as exc:
        stage_errors["bronze"] = str(exc)

        logger.exception(
            "Unexpected Bronze pipeline failure. "
            "Continuing with previously collected data."
        )

    # 2. Silver transformation
    try:
        silver_results = _run_stage(
            run_twelvedata_silver,
            storage,
        )

    except Exception as exc:
        stage_errors["silver"] = str(exc)

        logger.exception(
            "Silver pipeline failed. "
            "Continuing to Gold using previously collected Silver data."
        )

    # 3. Gold loading
    try:
        gold_results = _run_stage(
            run_twelvedata_gold,
            storage,
        )

    except Exception as exc:
        stage_errors["gold"] = str(exc)

        logger.exception("Gold pipeline failed.")

    logger.info(
        "Twelve Data pipeline finished | "
        "Bronze results: %s | Silver results: %s | Gold results: %s | "
        "Daily limit reached: %s | Stage errors: %s",
        len(bronze_results),
        len(silver_results),
        len(gold_results),
        daily_limit_reached,
        len(stage_errors),
    )

    logger.info("========== TWELVE DATA PIPELINE COMPLETED ==========")

    return {
        "bronze": bronze_results,
        "silver": silver_results,
        "gold": gold_results,
        "daily_limit_reached": daily_limit_reached,
        "stage_errors": stage_errors,
    }
