
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from pathlib import Path
from typing import Any

import polars as pl


def normalize_key(key: str) -> str:
    """Validate and normalize a relative storage object key."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Storage key must be a non-empty string.")

    if "\\" in key:
        raise ValueError("Storage keys must use forward slashes.")

    path = PurePosixPath(key)

    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid storage key: {key}")

    normalized = path.as_posix()

    if normalized in ("", "."):
        raise ValueError("Storage key must identify an object.")

    return normalized


def normalize_prefix(prefix: str) -> str:
    """Validate and normalize a relative storage prefix."""
    if not isinstance(prefix, str):
        raise TypeError("Storage prefix must be a string.")

    if not prefix:
        return ""

    if "\\" in prefix:
        raise ValueError("Storage prefixes must use forward slashes.")

    path = PurePosixPath(prefix)

    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid storage prefix: {prefix}")

    normalized = path.as_posix()

    return "" if normalized == "." else normalized.rstrip("/")


class StorageBackend(ABC):
    """Common interface for local and cloud object storage."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def list_files(
        self,
        prefix: str = "",
        pattern: str = "*",
    ) -> list[str]:
        pass

    @abstractmethod
    def read_bytes(self, key: str) -> bytes:
        pass

    @abstractmethod
    def write_bytes(
        self,
        key: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    def read_json(self, key: str) -> Any:
        pass

    @abstractmethod
    def write_json(
        self,
        key: str,
        data: Any,
        overwrite: bool = True,
    ) -> str:
        pass

    @abstractmethod
    def read_parquet(self, key: str) -> pl.DataFrame:
        pass

    @abstractmethod
    def write_parquet(
        self,
        key: str,
        df: pl.DataFrame,
        overwrite: bool = True,
    ) -> str:
        pass

    @abstractmethod
    def upload_file(
        self,
        source_path: Path,
        key: str,
        overwrite: bool = True,
    ) -> str:
        pass

    @abstractmethod
    def download_file(
        self,
        key: str,
        destination_path: Path,
    ) -> Path:
        pass
