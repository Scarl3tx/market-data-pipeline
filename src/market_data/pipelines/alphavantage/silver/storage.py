from pathlib import Path

import polars as pl

from market_data.pipelines.alphavantage.silver.validation import (
    validate_silver_news,
)


def save_silver_news(
    data: pl.DataFrame,
    file_path: Path,
    overwrite: bool = False,
) -> bool:
    """Validate and save Silver news data as Parquet."""

    validate_silver_news(data)

    file_path = Path(file_path)

    if file_path.exists() and not overwrite:
        return False

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = file_path.with_suffix(".tmp.pq")

    try:
        data.write_parquet(temp_path)

        # Verify the written Parquet file before replacing.
        saved_data = pl.read_parquet(temp_path)
        validate_silver_news(saved_data)

        temp_path.replace(file_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()

    return True


def load_silver_news(file_path: Path) -> pl.DataFrame:
    """Load and validate a Silver news Parquet file."""

    file_path = Path(file_path)

    data = pl.read_parquet(file_path)

    validate_silver_news(data)

    return data