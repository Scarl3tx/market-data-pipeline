
import os
from pathlib import Path

from market_data.paths import DATA_PATH
from market_data.storage.base import StorageBackend
from market_data.storage.local import LocalStorage
from market_data.storage.s3 import S3Storage


def get_storage(
    backend: str | None = None,
    root: Path | str | None = None,
) -> StorageBackend:
    selected_backend = (
        backend or os.getenv("MARKET_DATA_STORAGE_BACKEND", "local")
    ).strip().lower()

    if selected_backend == "local":
        return LocalStorage(root=root or DATA_PATH)

    if selected_backend == "s3":
        bucket = os.getenv("MARKET_DATA_S3_BUCKET")

        if not bucket:
            raise ValueError(
                "MARKET_DATA_S3_BUCKET must be configured for S3 storage."
            )

        prefix = os.getenv("MARKET_DATA_S3_PREFIX", "")

        return S3Storage(
            bucket=bucket,
            prefix=prefix,
        )

    raise ValueError(
        f"Unsupported storage backend: {selected_backend}"
    )
