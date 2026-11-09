
from market_data.pipelines.marketaux.bronze.coordinator import (
    run_marketaux_bronze_coordinator,
)
from market_data.pipelines.marketaux.silver.coordinator import (
    run_marketaux_silver_coordinator,
)
from market_data.pipelines.marketaux.gold.pipeline import (
    run_marketaux_gold_pipeline,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("marketaux.main")


def _run_stage(
    stage_name: str,
    stage_function,
    storage: StorageBackend | None = None,
) -> dict:
    try:
        if storage is None:
            results = stage_function()
        else:
            results = stage_function(storage=storage)

        if not isinstance(results, dict):
            raise TypeError(
                f"{stage_name} stage must return a dictionary."
            )

        return results

    except Exception as error:
        logger.exception(
            "Marketaux %s stage failed.",
            stage_name,
        )
        return {
            "status": "failed",
            "error": str(error),
        }


def run_marketaux_pipeline(
    storage: StorageBackend | None = None,
) -> dict:
    logger.info("Starting Marketaux pipeline.")

    bronze_results = _run_stage(
        "Bronze",
        run_marketaux_bronze_coordinator,
        storage,
    )

    silver_results = _run_stage(
        "Silver",
        run_marketaux_silver_coordinator,
        storage,
    )

    gold_results = _run_stage(
        "Gold",
        run_marketaux_gold_pipeline,
        storage,
    )

    results = {
        "bronze": bronze_results,
        "silver": silver_results,
        "gold": gold_results,
    }

    logger.info("Marketaux pipeline completed: %s", results)

    return results
