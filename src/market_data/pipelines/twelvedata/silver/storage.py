
from pathlib import Path

import polars as pl

from market_data.paths import (
    TWELVEDATA_STOCKS_BRONZE_S3_PREFIX,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("twelvedata.silver")


def load_stock_bronze_data(
    bronze_path: Path,
    symbol: str,
    storage: StorageBackend | None = None,
) -> pl.DataFrame:
    """Load and combine Bronze stock batches from local or object storage."""

    if not symbol.strip():
        raise ValueError("Stock symbol must not be empty.")

    if storage is None:
        stock_path = Path(bronze_path) / "stocks" / symbol

        if not stock_path.is_dir():
            logger.info(
                "No Bronze directory found for %s.",
                symbol,
            )
            return pl.DataFrame()

        files = sorted(
            stock_path.glob(f"{symbol}_*_raw.pq")
        )

        if not files:
            logger.info(
                "No Bronze Parquet files found for %s.",
                symbol,
            )
            return pl.DataFrame()

        logger.info(
            "Loading %s local Bronze files for %s.",
            len(files),
            symbol,
        )

        dataframes = [
            pl.read_parquet(file)
            for file in files
        ]

    else:
        prefix = (
            f"{TWELVEDATA_STOCKS_BRONZE_S3_PREFIX}/{symbol}"
        )

        keys = storage.list_files(
            prefix=prefix,
            pattern=f"{symbol}_*_raw.pq",
        )

        if not keys:
            logger.info(
                "No Bronze Parquet objects found for %s.",
                symbol,
            )
            return pl.DataFrame()

        logger.info(
            "Loading %s Bronze objects for %s.",
            len(keys),
            symbol,
        )

        dataframes = [
            storage.read_parquet(key)
            for key in sorted(keys)
        ]

    df = pl.concat(dataframes)

    original_rows = df.height

    # Keep the last occurrence from the sorted batch sequence.
    # Sort the resulting unique rows by date afterwards.
    df = (
        df.unique(
            subset=["date"],
            keep="last",
            maintain_order=True,
        )
        .sort("date")
    )

    logger.info(
        "%s Bronze data loaded: %s original rows, %s unique rows.",
        symbol,
        original_rows,
        df.height,
    )

    return df
