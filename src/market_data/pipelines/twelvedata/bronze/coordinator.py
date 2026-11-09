
from market_data.api.twelvedata.client import TwelveDataClient
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.bronze.update import (
    update_stocks,
)


logger = get_pipeline_logger("twelvedata.bronze")


def run_twelvedata_bronze(
    provider=None,
    storage: StorageBackend | None = None,
) -> list[dict]:
    """Coordinate the Twelve Data Bronze stock ingestion."""

    logger.info("========== TWELVE DATA BRONZE STARTED ==========")

    if provider is None:
        if storage is None:
            provider = TwelveDataClient()
        else:
            provider = TwelveDataClient(storage=storage)

    try:
        if storage is None:
            results = update_stocks(provider=provider)
        else:
            results = update_stocks(
                provider=provider,
                storage=storage,
            )

    except TwelveDataDailyLimitError:
        logger.warning(
            "Twelve Data daily credit limit reached. "
            "Stopping Bronze ingestion."
        )
        raise

    successful = sum(
        1 for result in results
        if result.get("success") and not result.get("skipped")
    )

    failed = sum(
        1 for result in results
        if not result.get("success")
    )

    skipped = sum(
        1 for result in results
        if result.get("skipped")
    )

    logger.info(
        "Bronze ingestion completed | "
        "Total: %s | Successful: %s | Failed: %s | Skipped: %s",
        len(results),
        successful,
        failed,
        skipped,
    )

    for result in results:
        if not result.get("success"):
            logger.error(
                "%s Bronze ingestion failed: %s",
                result.get("symbol", "Unknown"),
                result.get("error", "Unknown error"),
            )

    logger.info("========== TWELVE DATA BRONZE COMPLETED ==========")

    return results
