from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend

from market_data.pipelines.marketaux.bronze.downloader import (
    run_marketaux_bronze,
)


logger = get_pipeline_logger("marketaux.bronze")


def run_marketaux_bronze_coordinator(
    client=None,
    state=None,
    stocks=None,
    storage: StorageBackend | None = None,
) -> dict:
    """Coordinate Marketaux Bronze news ingestion."""

    logger.info("========== MARKETAUX BRONZE STARTED ==========")

    results = run_marketaux_bronze(
        client=client,
        state=state,
        stocks=stocks,
        storage=storage,
    )

    successful = len(results.get("successful", []))
    failed = len(results.get("failed", []))
    status = results.get("status", "unknown")

    logger.info(
        "Marketaux Bronze completed | "
        "Status: %s | Pages saved: %s | Failures: %s",
        status,
        successful,
        failed,
    )

    for failure in results.get("failed", []):
        logger.error(
            "%s Marketaux ingestion failed: %s",
            failure.get("symbol", "Unknown"),
            failure.get("error", "Unknown error"),
        )

    logger.info("========== MARKETAUX BRONZE COMPLETED ==========")

    return results