
import os
import tempfile
from pathlib import Path

import polars as pl

from market_data.pipelines.marketaux.silver.validation import (
    MarketauxSilverValidationError,
    validate_silver_news,
)
from market_data.storage.base import StorageBackend


def save_silver_news(
    data: pl.DataFrame,
    file_path: Path | str,
    overwrite: bool = False,
    storage: StorageBackend | None = None,
) -> bool:
    validate_silver_news(data)

    # S3 or another configured storage backend.
    if storage is not None:
        key = str(file_path).replace("\\", "/").lstrip("/")

        if storage.exists(key) and not overwrite:
            return False

        storage.write_parquet(
            key,
            data,
            overwrite=overwrite,
        )

        # Verify the persisted Parquet data.
        saved_data = storage.read_parquet(key)
        validate_silver_news(saved_data)

        return True

    # Existing local filesystem implementation.
    file_path = Path(file_path)

    if file_path.exists() and not overwrite:
        return False

    file_path.parent.mkdir(parents=True, exist_ok=True)

    file_descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{file_path.name}.",
        suffix=".tmp.pq",
        dir=file_path.parent,
    )
    os.close(file_descriptor)

    temp_path = Path(temp_name)

    try:
        data.write_parquet(temp_path)

        saved_data = pl.read_parquet(temp_path)
        validate_silver_news(saved_data)

        if overwrite:
            temp_path.replace(file_path)
        else:
            try:
                os.link(temp_path, file_path)
            except FileExistsError:
                return False

        return True

    finally:
        if temp_path.exists():
            temp_path.unlink()


def load_silver_news(
    file_path: Path | str,
    storage: StorageBackend | None = None,
) -> pl.DataFrame:
    if storage is not None:
        key = str(file_path).replace("\\", "/").lstrip("/")

        if not storage.exists(key):
            raise FileNotFoundError(
                f"Silver news file does not exist: {key}"
            )

        try:
            data = storage.read_parquet(key)
        except Exception as error:
            raise MarketauxSilverValidationError(
                f"Unable to read Silver news Parquet file: {key}. "
                f"Error: {error}"
            ) from error

        validate_silver_news(data)
        return data

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Silver news file does not exist: {file_path}"
        )

    try:
        data = pl.read_parquet(file_path)
    except Exception as error:
        raise MarketauxSilverValidationError(
            f"Unable to read Silver news Parquet file: {file_path}. "
            f"Error: {error}"
        ) from error

    validate_silver_news(data)

    return data
