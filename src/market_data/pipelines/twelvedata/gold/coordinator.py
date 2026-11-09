
from collections import Counter

from market_data.config import load_stocks_config
from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.gold.stocks import (
    run_stock_gold,
)
from market_data.logging_config import get_pipeline_logger


logger = get_pipeline_logger("twelvedata.gold")


def run_twelvedata_gold(
    symbols: set[str] | None = None,
    storage: StorageBackend | None = None,
) -> list[dict]:

    logger.info("========== TWELVE DATA GOLD ==========")

    stocks = load_stocks_config()

    if symbols is not None:
        stocks = [
            stock for stock in stocks
            if stock["symbol"] in symbols
        ]

    results = []

    for stock in stocks:
        symbol = stock["symbol"]

        logger.info("Processing %s", symbol)

        if storage is None:
            result = run_stock_gold(symbol=symbol)
        else:
            result = run_stock_gold(
                symbol=symbol,
                storage=storage,
            )

        results.append(result)

        if result["success"] and result.get("skipped"):
            logger.warning(
                "%s skipped | %s",
                symbol,
                result.get("reason", "No reason provided"),
            )

        elif result["success"]:
            rows_loaded = result.get("rows", 0)
            rejected_rows = result.get("rejected_rows", 0)
            quarantined_rows = result.get("quarantined_rows", 0)
            validation_reasons = result.get(
                "validation_reasons",
                {},
            )

            logger.info(
                "%s completed | Rows loaded: %s | "
                "Rows rejected: %s",
                symbol,
                rows_loaded,
                rejected_rows,
            )

            if rejected_rows:
                if result.get("quarantine_error"):
                    logger.error(
                        "%s quarantine failed | Rejected rows: %s | %s",
                        symbol,
                        rejected_rows,
                        result["quarantine_error"],
                    )
                else:
                    logger.info(
                        "%s quarantine completed | "
                        "Rejected rows: %s | "
                        "New unique records: %s",
                        symbol,
                        rejected_rows,
                        quarantined_rows,
                    )

            if validation_reasons:
                logger.warning(
                    "%s validation issues | %s",
                    symbol,
                    validation_reasons,
                )

        else:
            logger.error(
                "%s failed | %s",
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

    total_rows_loaded = sum(
        result.get("rows", 0)
        for result in results
        if result["success"]
    )

    total_rows_rejected = sum(
        result.get("rejected_rows", 0)
        for result in results
    )

    quarantine_failed_rows = sum(
        result.get("rejected_rows", 0)
        for result in results
        if result.get("quarantine_error")
    )

    quarantine_successful_rows = (
        total_rows_rejected - quarantine_failed_rows
    )

    quarantine_failed_symbols = sum(
        1 for result in results
        if result.get("quarantine_error")
    )

    rejection_reasons = Counter()

    for result in results:
        rejection_reasons.update(
            result.get("validation_reasons", {})
        )

    logger.info(
        "Gold completed | Successful: %s | Failed: %s | "
        "Skipped: %s",
        successful,
        failed,
        skipped,
    )

    logger.info(
        "Gold data quality | Rows loaded: %s | "
        "Rows rejected: %s",
        total_rows_loaded,
        total_rows_rejected,
    )

    logger.info(
        "Gold quarantine summary | Successfully quarantined: %s | "
        "Failed to quarantine: %s | Symbols with quarantine failures: %s",
        quarantine_successful_rows,
        quarantine_failed_rows,
        quarantine_failed_symbols,
    )

    if rejection_reasons:
        logger.warning(
            "Gold validation summary | %s",
            dict(sorted(rejection_reasons.items())),
        )

    return results
