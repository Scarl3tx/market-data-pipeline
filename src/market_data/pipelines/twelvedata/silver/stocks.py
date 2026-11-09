
import polars as pl

from market_data.paths import (
    TWELVEDATA_BRONZE_PATH,
    STOCKS_SILVER_PATH,
)

from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.silver.storage import (
    load_stock_bronze_data,
)

from market_data.pipelines.twelvedata.silver.transform import (
    transform_stock_silver,
)

from market_data.pipelines.twelvedata.silver.validation import (
    validate_stock_silver,
)

from market_data.pipelines.twelvedata.silver.save import (
    get_silver_output_path,
    save_stock_silver,
)

from market_data.logging_config import get_pipeline_logger

logger = get_pipeline_logger("twelvedata.silver")


def run_stock_silver(
    symbol: str,
    bronze_path=TWELVEDATA_BRONZE_PATH,
    silver_path=STOCKS_SILVER_PATH,
    storage: StorageBackend | None = None,
) -> dict:

    logger.info(
        "Starting Silver transformation for %s.",
        symbol,
    )

    try:
        # 1. Load Bronze data
        bronze_df = load_stock_bronze_data(
            bronze_path=bronze_path,
            symbol=symbol,
            storage=storage,
        )

        if bronze_df.is_empty():
            logger.info(
                "Skipping Silver transformation for %s: no Bronze data found.",
                symbol,
            )

            return {
                "symbol": symbol,
                "success": True,
                "skipped": True,
                "reason": "No Bronze data found",
            }

        logger.info(
            "%s Bronze data loaded: %s rows.",
            symbol,
            bronze_df.height,
        )

        # 2. Transform
        silver_df = transform_stock_silver(bronze_df)

        logger.info(
            "%s transformation completed: %s rows.",
            symbol,
            silver_df.height,
        )

        # 3. Validate
        validate_stock_silver(silver_df)

        logger.info(
            "%s Silver validation passed.",
            symbol,
        )

        # 4. Determine output location
        output_path = get_silver_output_path(
            silver_path,
            symbol,
        )

        storage_key = (
            f"silver/stocks/{symbol}/{symbol}_silver.pq"
            if storage is not None
            else None
        )

        # 5. Save
        save_stock_silver(
            df=silver_df,
            output_path=output_path,
            storage=storage,
            storage_key=storage_key,
        )

        result_path = storage_key or output_path

        logger.info(
            "%s Silver pipeline completed: %s rows.",
            symbol,
            silver_df.height,
        )

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": silver_df.height,
            "output_path": str(result_path),
        }

    except Exception as exc:
        logger.exception(
            "Silver pipeline failed for %s.",
            symbol,
        )

        return {
            "symbol": symbol,
            "success": False,
            "skipped": False,
            "error": str(exc),
        }
