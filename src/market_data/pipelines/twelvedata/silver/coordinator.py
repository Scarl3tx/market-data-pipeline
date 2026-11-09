
from market_data.config import load_stocks_config
from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.silver.stocks import (
    run_stock_silver,
)

from market_data.logging_config import get_pipeline_logger

logger = get_pipeline_logger("twelvedata.silver")


def run_twelvedata_silver(
    symbols: set[str] | None = None,
    storage: StorageBackend | None = None,
) -> list[dict]:
    """Coordinate the Twelve Data Silver transformation."""

    logger.info("========== TWELVE DATA SILVER STARTED ==========")

    stocks = load_stocks_config()

    if symbols is not None:
        stocks = [
            stock for stock in stocks
            if stock["symbol"] in symbols
        ]

    logger.info(
        "Processing %s configured stocks.",
        len(stocks),
    )

    results = []

    for stock in stocks:
        symbol = stock["symbol"]

        if storage is None:
            result = run_stock_silver(symbol=symbol)
        else:
            result = run_stock_silver(
                symbol=symbol,
                storage=storage,
            )

        results.append(result)

        if not result["success"]:
            logger.error(
                "%s Silver processing failed: %s",
                symbol,
                result.get("error", "Unknown error"),
            )

    successful = sum(
        1 for result in results
        if result["success"] and not result.get("skipped")
    )

    failed = sum(
        1 for result in results
        if not result["success"]
    )

    skipped = sum(
        1 for result in results
        if result.get("skipped")
    )

    logger.info(
        "Silver processing completed | "
        "Total: %s | Successful: %s | Failed: %s | Skipped: %s",
        len(results),
        successful,
        failed,
        skipped,
    )

    logger.info("========== TWELVE DATA SILVER COMPLETED ==========")

    return results
