
from pathlib import Path

import polars as pl

from market_data.paths import STOCKS_SILVER_PATH
from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.gold.validation import (
    validate_stock_data,
)
from market_data.pipelines.twelvedata.gold.quarantine import (
    save_rejected_stock_data,
)
from market_data.database.loader import load_stocks_to_gold
from market_data.logging_config import get_pipeline_logger


logger = get_pipeline_logger("twelvedata.gold")

GOLD_INDICATOR_COLUMNS = {
    "rsi_14": pl.Float64,
    "sma_50": pl.Float64,
    "sma_200": pl.Float64,
}


def _load_silver_data(
    symbol: str,
    silver_path: Path,
    storage: StorageBackend | None = None,
) -> pl.DataFrame | None:
    if storage is not None:
        key = (
            f"silver/stocks/{symbol}/"
            f"{symbol}_silver.pq"
        )

        if not storage.exists(key):
            return None

        return storage.read_parquet(key)

    stock_path = silver_path / symbol

    if not stock_path.exists():
        return None

    files = sorted(
        stock_path.glob(f"{symbol}_silver.pq")
    )

    if not files:
        return None

    dataframes = [
        pl.read_parquet(file)
        for file in files
    ]

    return pl.concat(dataframes)


def _prepare_gold_data(
    df: pl.DataFrame,
) -> pl.DataFrame:
    missing_columns = [
        pl.lit(None, dtype=dtype).alias(column)
        for column, dtype in GOLD_INDICATOR_COLUMNS.items()
        if column not in df.columns
    ]

    if missing_columns:
        df = df.with_columns(missing_columns)

    return df


def _save_gold_data(
    symbol: str,
    valid_df: pl.DataFrame,
    storage: StorageBackend,
) -> int:
    """
    Merge valid records into the persistent Gold Parquet dataset.

    Existing records are retained. Incoming records replace
    matching asset_name/date keys.
    """

    gold_key = (
        f"gold/stocks/{symbol}/{symbol}_gold.pq"
    )

    incoming_df = _prepare_gold_data(valid_df)

    if storage.exists(gold_key):
        existing_df = storage.read_parquet(gold_key)

        combined_df = pl.concat(
            [existing_df, incoming_df],
            how="diagonal_relaxed",
        )
    else:
        combined_df = incoming_df

    combined_df = (
        combined_df
        .sort("date")
        .unique(
            subset=["asset_name", "date"],
            keep="last",
        )
        .sort("date")
    )

    storage.write_parquet(
        gold_key,
        combined_df,
        overwrite=True,
    )

    # Verify the persisted dataset.
    saved_df = storage.read_parquet(gold_key)

    if (
        saved_df.height != combined_df.height
        or saved_df.columns != combined_df.columns
    ):
        raise IOError(
            f"Gold storage verification failed for {symbol}."
        )

    return valid_df.height


def run_stock_gold(
    symbol: str,
    silver_path=STOCKS_SILVER_PATH,
    connection=None,
    storage: StorageBackend | None = None,
) -> dict:
    try:
        silver_df = _load_silver_data(
            symbol=symbol,
            silver_path=Path(silver_path),
            storage=storage,
        )

        if silver_df is None:
            return {
                "symbol": symbol,
                "success": True,
                "skipped": True,
                "reason": "No Silver data found",
            }

        if silver_df.is_empty():
            return {
                "symbol": symbol,
                "success": True,
                "skipped": True,
                "reason": "No Silver data found",
            }

        silver_df = (
            silver_df
            .sort("date")
            .unique(
                subset=["asset_name", "date"],
                keep="last",
            )
        )

        validation = validate_stock_data(silver_df)

        valid_df = validation["valid"]
        invalid_df = validation["invalid"]

        rejected_rows = validation["rejected_rows"]
        validation_reasons = validation["reasons"]

        quarantined_rows = 0
        quarantine_error = None

        if rejected_rows:
            logger.warning(
                "%s | Rejected %s invalid rows | Reasons: %s",
                symbol,
                rejected_rows,
                validation_reasons,
            )

            try:
                if storage is None:
                    quarantined_rows = save_rejected_stock_data(
                        symbol=symbol,
                        rejected_df=invalid_df,
                    )
                else:
                    quarantined_rows = save_rejected_stock_data(
                        symbol=symbol,
                        rejected_df=invalid_df,
                        storage=storage,
                    )

                logger.info(
                    "%s | Newly quarantined records: %s",
                    symbol,
                    quarantined_rows,
                )

            except Exception as exc:
                quarantine_error = str(exc)

                logger.exception(
                    "%s | Failed to save rejected records "
                    "to quarantine",
                    symbol,
                )

        if valid_df.is_empty():
            rows_loaded = 0

        else:
            if storage is not None:
                _save_gold_data(
                    symbol=symbol,
                    valid_df=valid_df,
                    storage=storage,
                )

            rows_loaded = load_stocks_to_gold(
                df=valid_df,
                connection=connection,
            )

        result = {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": rows_loaded,
            "rejected_rows": rejected_rows,
            "quarantined_rows": quarantined_rows,
            "validation_reasons": validation_reasons,
        }

        if quarantine_error is not None:
            result["quarantine_error"] = quarantine_error

        return result

    except Exception as exc:
        logger.exception(
            "%s Gold pipeline failed",
            symbol,
        )

        return {
            "symbol": symbol,
            "success": False,
            "skipped": False,
            "error": str(exc),
        }
