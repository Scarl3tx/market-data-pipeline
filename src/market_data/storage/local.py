
import io
import json
import os
import tempfile
from pathlib import Path

import polars as pl

from market_data.storage.base import (
    StorageBackend,
    normalize_key,
    normalize_prefix,
)


class LocalStorage(StorageBackend):
    """Filesystem implementation of the shared storage interface."""

    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        normalized = normalize_key(key)
        path = (self.root / Path(*normalized.split("/"))).resolve()

        if not path.is_relative_to(self.root):
            raise ValueError(f"Storage key escapes root: {key}")

        return path

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_files(
        self,
        prefix: str = "",
        pattern: str = "*",
    ) -> list[str]:
        normalized = normalize_prefix(prefix)
        directory = (
            self.root / Path(*normalized.split("/"))
            if normalized
            else self.root
        )

        if not directory.is_dir():
            return []

        return sorted(
            path.relative_to(self.root).as_posix()
            for path in directory.rglob(pattern)
            if path.is_file()
        )

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def write_bytes(
        self,
        key: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        if not isinstance(data, bytes):
            raise TypeError("Storage data must be bytes.")

        output_path = self._path(key)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists() and not overwrite:
            raise FileExistsError(output_path)

        temporary_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=output_path.parent,
                prefix=f".{output_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                temporary_file.write(data)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            if not overwrite and output_path.exists():
                raise FileExistsError(output_path)

            os.replace(temporary_path, output_path)
            return key

        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def delete(self, key: str) -> bool:
        path = self._path(key)

        if not path.is_file():
            return False

        path.unlink()
        return True

    def read_json(self, key: str):
        return json.loads(self.read_bytes(key).decode("utf-8"))

    def write_json(
        self,
        key: str,
        data,
        overwrite: bool = True,
    ) -> str:
        serialized = json.dumps(
            data,
            indent=4,
            ensure_ascii=False,
        ).encode("utf-8")

        return self.write_bytes(
            key,
            serialized,
            overwrite=overwrite,
        )

    def read_parquet(self, key: str) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(self.read_bytes(key)))

    def write_parquet(
        self,
        key: str,
        df: pl.DataFrame,
        overwrite: bool = True,
    ) -> str:
        buffer = io.BytesIO()
        df.write_parquet(buffer)

        return self.write_bytes(
            key,
            buffer.getvalue(),
            overwrite=overwrite,
        )

    def upload_file(
        self,
        source_path: Path,
        key: str,
        overwrite: bool = True,
    ) -> str:
        source_path = Path(source_path)

        if not source_path.is_file():
            raise FileNotFoundError(source_path)

        return self.write_bytes(
            key,
            source_path.read_bytes(),
            overwrite=overwrite,
        )

    def download_file(
        self,
        key: str,
        destination_path: Path,
    ) -> Path:
        destination_path = Path(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=destination_path.parent,
                prefix=f".{destination_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                temporary_file.write(self.read_bytes(key))
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            os.replace(temporary_path, destination_path)
            return destination_path

        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
