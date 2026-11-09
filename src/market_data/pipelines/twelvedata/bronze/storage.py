from datetime import date, datetime
from pathlib import Path

import polars as pl

from market_data.paths import (
    TWELVEDATA_BRONZE_PATH,
    TWELVEDATA_STOCKS_BRONZE_S3_PREFIX,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("twelvedata.bronze")


def get_stock_output_path(
    bronze_path: Path = TWELVEDATA_BRONZE_PATH,
    symbol: str = "",
    earliest_date: str = "",
    latest_date: str = "",
) -> Path:
    """Build the local output path for a stock Bronze batch."""

    if not symbol.strip():
        raise ValueError("Stock symbol must not be empty.")

    return (
        Path(bronze_path)
        / "stocks"
        / symbol
        / f"{symbol}_{earliest_date}_{latest_date}_raw.pq"
    )


def save_stock_batch(
    df: pl.DataFrame,
    output_path: Path,
) -> Path:
    """Save a stock Bronze batch without overwriting existing data."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        raise FileExistsError(
            f"Bronze batch already exists: {output_path}"
        )

    try:
        df.write_parquet(output_path)
    except Exception:
        if output_path.exists():
            output_path.unlink()
        raise

    logger.info(
        "Saved stock Bronze batch: %s (%s rows).",
        output_path,
        df.height,
    )

    return output_path


def _get_stock_dates(
    bronze_path: Path,
    symbol: str,
    storage: StorageBackend | None = None,
) -> tuple:
    """Discover the earliest and latest dates for a stock."""

    if not symbol.strip():
        raise ValueError("Stock symbol must not be empty.")

    if storage is None:
        stock_path = Path(bronze_path) / "stocks" / symbol

        if not stock_path.is_dir():
            return None, None

        files = sorted(
            stock_path.glob(f"{symbol}_*_raw.pq")
        )

        if not files:
            return None, None

        dataframes = [
            pl.read_parquet(file, columns=["date"])
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
            return None, None

        dataframes = [
            storage.read_parquet(key).select("date")
            for key in sorted(keys)
        ]

    combined = pl.concat(dataframes)

    if combined.is_empty():
        return None, None

    oldest_date, latest_date = combined.select(
        pl.col("date").min().alias("oldest_date"),
        pl.col("date").max().alias("latest_date"),
    ).row(0)

    if isinstance(oldest_date, datetime):
        oldest_date = oldest_date.date()

    if isinstance(latest_date, datetime):
        latest_date = latest_date.date()

    return oldest_date, latest_date


def get_latest_stock_date(
    bronze_path: Path = TWELVEDATA_BRONZE_PATH,
    symbol: str = "",
    storage: StorageBackend | None = None,
):
    """Return the latest available date for a stock."""

    _, latest_date = _get_stock_dates(
        bronze_path=bronze_path,
        symbol=symbol,
        storage=storage,
    )

    return latest_date


def get_oldest_stock_date(
    bronze_path: Path = TWELVEDATA_BRONZE_PATH,
    symbol: str = "",
    storage: StorageBackend | None = None,
):
    """Return the earliest available date for a stock."""

    oldest_date, _ = _get_stock_dates(
        bronze_path=bronze_path,
        symbol=symbol,
        storage=storage,
    )

    return oldest_date
