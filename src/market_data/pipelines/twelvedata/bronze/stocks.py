
from pathlib import Path
from typing import Any

from market_data.paths import (
    TWELVEDATA_BRONZE_PATH,
    TWELVEDATA_STOCKS_BRONZE_S3_PREFIX,
)
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
    TwelveDataAPIError,
)
from market_data.pipelines.twelvedata.bronze.storage import (
    get_stock_output_path,
    save_stock_batch,
)
from market_data.storage.base import StorageBackend
from market_data.utils.validation import validate_stock_dataframe
from market_data.logging_config import get_pipeline_logger

logger = get_pipeline_logger("twelvedata.bronze")


def ingest_stock_batch(
    provider: Any,
    symbol: str,
    start_date: str | None,
    end_date: str,
    bronze_path: Path = TWELVEDATA_BRONZE_PATH,
    storage: StorageBackend | None = None,
) -> dict[str, Any]:

    logger.info(
        "Starting Bronze ingestion for %s: %s to %s",
        symbol,
        start_date or "earliest available",
        end_date,
    )

    try:
        df = provider.get_stock_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    except TwelveDataDailyLimitError:
        logger.warning(
            "Twelve Data daily limit reached while ingesting %s.",
            symbol,
        )
        raise

    except TwelveDataAPIError as exc:
        logger.exception(
            "Twelve Data API request failed for %s. "
            "Exception type=%s, module=%s, args=%r",
            symbol,
            type(exc).__qualname__,
            type(exc).__module__,
            exc.args,
        )
        return {
            "symbol": symbol,
            "success": False,
            "rows": 0,
            "earliest_date": None,
            "latest_date": None,
            "has_data": False,
            "error": str(exc),
        }

    if df.is_empty():
        logger.info(
            "No data returned for %s between %s and %s.",
            symbol,
            start_date or "earliest available",
            end_date,
        )

        return {
            "symbol": symbol,
            "success": True,
            "rows": 0,
            "earliest_date": None,
            "latest_date": None,
            "has_data": False,
        }

    validate_stock_dataframe(df, symbol)

    earliest_date = df["date"].min().strftime("%Y%m%d")
    latest_date = df["date"].max().strftime("%Y%m%d")

    filename = (
        f"{symbol}_{earliest_date}_{latest_date}_raw.pq"
    )

    local_output_path: Path | None = None
    output_key: str | None = None

    if storage is None:
        local_output_path = get_stock_output_path(
            bronze_path=bronze_path,
            symbol=symbol,
            earliest_date=earliest_date,
            latest_date=latest_date,
        )

        already_exists = local_output_path.exists()

        output_path = local_output_path

    else:
        output_key = (
            f"{TWELVEDATA_STOCKS_BRONZE_S3_PREFIX}"
            f"/{symbol}/{filename}"
        )

        already_exists = storage.exists(output_key)

        output_path = output_key

    if already_exists:
        logger.info(
            "Skipping existing Bronze batch for %s: %s",
            symbol,
            output_path,
        )

        return {
            "symbol": symbol,
            "success": True,
            "rows": df.height,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "has_data": True,
            "skipped": True,
            "output_path": output_path,
        }

    if storage is None:
        assert local_output_path is not None

        save_stock_batch(
            df=df,
            output_path=local_output_path,
        )

    else:
        assert output_key is not None

        storage.write_parquet(
            output_key,
            df,
            overwrite=False,
        )

    logger.info(
        "Bronze ingestion completed for %s: %s rows, %s to %s.",
        symbol,
        df.height,
        earliest_date,
        latest_date,
    )

    return {
        "symbol": symbol,
        "success": True,
        "rows": df.height,
        "earliest_date": earliest_date,
        "latest_date": latest_date,
        "has_data": True,
        "skipped": False,
        "output_path": output_path,
    }
