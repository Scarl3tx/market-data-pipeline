
from pathlib import Path
from uuid import uuid4

import polars as pl

from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend

logger = get_pipeline_logger("twelvedata.silver")


def get_silver_output_path(
    silver_path: Path,
    symbol: str,
) -> Path:
    return silver_path / symbol / f"{symbol}_silver.pq"


def save_stock_silver(
    df: pl.DataFrame,
    output_path: Path,
    storage: StorageBackend | None = None,
    storage_key: str | None = None,
) -> Path | str:
    """Save and verify a Silver snapshot."""

    if df.is_empty():
        raise ValueError("Cannot save an empty Silver DataFrame.")

    if storage is not None:
        if not storage_key:
            raise ValueError(
                "storage_key is required when using a storage backend."
            )

        try:
            storage.write_parquet(
                storage_key,
                df,
                overwrite=True,
            )

            saved_df = storage.read_parquet(storage_key)

            if saved_df.shape != df.shape:
                raise ValueError(
                    "Saved Silver object failed shape verification."
                )

            if saved_df.columns != df.columns:
                raise ValueError(
                    "Saved Silver object failed column verification."
                )

            logger.info(
                "Saved Silver snapshot: %s (%s rows).",
                storage_key,
                df.height,
            )

            return storage_key

        except Exception:
            logger.exception(
                "Failed to save Silver object: %s",
                storage_key,
            )
            raise

    output_path = Path(output_path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = output_path.with_name(
        f"{output_path.stem}.{uuid4().hex}.tmp{output_path.suffix}"
    )

    try:
        df.write_parquet(temporary_path)

        saved_df = pl.read_parquet(temporary_path)

        if saved_df.shape != df.shape:
            raise ValueError(
                "Temporary Silver file failed shape verification."
            )

        if saved_df.columns != df.columns:
            raise ValueError(
                "Temporary Silver file failed column verification."
            )

        temporary_path.replace(output_path)

        logger.info(
            "Saved Silver snapshot: %s (%s rows).",
            output_path,
            df.height,
        )

        return output_path

    except Exception:
        logger.exception(
            "Failed to save Silver snapshot: %s",
            output_path,
        )
        raise

    finally:
        if temporary_path.exists():
            temporary_path.unlink()
