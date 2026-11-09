
from pathlib import Path
from uuid import uuid4

import polars as pl

from market_data.paths import (
    TWELVEDATA_STOCKS_QUARANTINE_PATH,
)
from market_data.storage.base import StorageBackend


def _validate_quarantine_data(
    rejected_df: pl.DataFrame,
) -> None:
    required_columns = {
        "asset_name",
        "date",
        "validation_errors",
    }

    missing_columns = required_columns - set(rejected_df.columns)

    if missing_columns:
        raise ValueError(
            "Missing quarantine columns: "
            + ", ".join(sorted(missing_columns))
        )


def _merge_quarantine_data(
    existing_df: pl.DataFrame | None,
    rejected_df: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    if existing_df is None:
        existing_keys = set()
        combined_df = rejected_df
    else:
        existing_keys = set(
            existing_df
            .select(["asset_name", "date"])
            .unique()
            .iter_rows()
        )

        combined_df = pl.concat(
            [existing_df, rejected_df],
            how="diagonal_relaxed",
        )

    incoming_keys = set(
        rejected_df
        .select(["asset_name", "date"])
        .unique()
        .iter_rows()
    )

    new_records = len(incoming_keys - existing_keys)

    combined_df = (
        combined_df
        .sort("date")
        .unique(
            subset=["asset_name", "date"],
            keep="last",
        )
        .sort("date")
    )

    return combined_df, new_records


def save_rejected_stock_data(
    symbol: str,
    rejected_df: pl.DataFrame,
    quarantine_path: Path = TWELVEDATA_STOCKS_QUARANTINE_PATH,
    storage: StorageBackend | None = None,
) -> int:
    """
    Append rejected stock records to quarantine.

    Records are deduplicated by asset_name and date.
    Repeated records replace their previous validation details.

    Returns the number of newly added unique records.
    """

    if rejected_df.is_empty():
        return 0

    _validate_quarantine_data(rejected_df)

    if storage is not None:
        storage_key = (
            f"quarantine/twelvedata/stocks/"
            f"{symbol}/{symbol}_rejected.pq"
        )

        existing_df = (
            storage.read_parquet(storage_key)
            if storage.exists(storage_key)
            else None
        )

        combined_df, new_records = _merge_quarantine_data(
            existing_df,
            rejected_df,
        )

        storage.write_parquet(
            storage_key,
            combined_df,
            overwrite=True,
        )

        return new_records

    quarantine_path = Path(quarantine_path)

    quarantine_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_file = (
        quarantine_path / f"{symbol}_rejected.pq"
    )

    existing_df = (
        pl.read_parquet(quarantine_file)
        if quarantine_file.exists()
        else None
    )

    combined_df, new_records = _merge_quarantine_data(
        existing_df,
        rejected_df,
    )

    temporary_file = quarantine_file.with_name(
        f".{quarantine_file.stem}_{uuid4().hex}.tmp.pq"
    )

    try:
        combined_df.write_parquet(temporary_file)
        temporary_file.replace(quarantine_file)

    finally:
        if temporary_file.exists():
            temporary_file.unlink()

    return new_records
