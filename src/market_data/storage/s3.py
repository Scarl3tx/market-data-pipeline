
import io
import json
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import boto3
import polars as pl
from botocore.exceptions import ClientError

from market_data.storage.base import (
    StorageBackend,
    normalize_key,
    normalize_prefix,
)


class S3Storage(StorageBackend):
    """Amazon S3 implementation of the shared storage interface."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        client=None,
    ):
        if not bucket or not bucket.strip():
            raise ValueError("S3 bucket name cannot be empty.")

        self.bucket = bucket.strip()
        self.prefix = normalize_prefix(prefix)

        self.client = client or boto3.client("s3")

    def _key(self, key: str) -> str:
        normalized = normalize_key(key)

        if self.prefix:
            return f"{self.prefix}/{normalized}"

        return normalized

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=self._key(key),
            )
            return True

        except ClientError as exc:
            status = exc.response.get(
                "ResponseMetadata", {}
            ).get("HTTPStatusCode")

            code = exc.response.get(
                "Error", {}
            ).get("Code")

            if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
                return False

            raise

    def list_files(
        self,
        prefix: str = "",
        pattern: str = "*",
    ) -> list[str]:
        normalized = normalize_prefix(prefix)

        if normalized:
            search_prefix = f"{normalized}/"
        else:
            search_prefix = ""

        full_prefix = (
            f"{self.prefix}/{search_prefix}"
            if self.prefix and search_prefix
            else (
                f"{self.prefix}/"
                if self.prefix
                else search_prefix
            )
        )

        paginator = self.client.get_paginator("list_objects_v2")

        results = []

        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=full_prefix,
        ):
            for item in page.get("Contents", []):
                full_key = item["Key"]

                if self.prefix:
                    object_key = full_key[
                        len(self.prefix) + 1:
                    ]
                else:
                    object_key = full_key

                if not object_key:
                    continue

                filename = object_key.rsplit("/", 1)[-1]

                if fnmatch(filename, pattern):
                    results.append(object_key)

        return sorted(results)

    def read_bytes(self, key: str) -> bytes:
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=self._key(key),
        )

        return response["Body"].read()

    def write_bytes(
        self,
        key: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        if not isinstance(data, bytes):
            raise TypeError("Storage data must be bytes.")

        full_key = self._key(key)

        if not overwrite and self.exists(key):
            raise FileExistsError(
                f"s3://{self.bucket}/{full_key}"
            )

        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=data,
        )

        return key

    def delete(self, key: str) -> bool:
        existed = self.exists(key)

        if not existed:
            return False

        self.client.delete_object(
            Bucket=self.bucket,
            Key=self._key(key),
        )

        return True

    def read_json(self, key: str) -> Any:
        return json.loads(
            self.read_bytes(key).decode("utf-8")
        )

    def write_json(
        self,
        key: str,
        data: Any,
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
        return pl.read_parquet(
            io.BytesIO(self.read_bytes(key))
        )

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

        full_key = self._key(key)

        if not overwrite and self.exists(key):
            raise FileExistsError(
                f"s3://{self.bucket}/{full_key}"
            )

        self.client.upload_file(
            str(source_path),
            self.bucket,
            full_key,
        )

        return key

    def download_file(
        self,
        key: str,
        destination_path: Path,
    ) -> Path:
        destination_path = Path(destination_path)
        destination_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.client.download_file(
            self.bucket,
            self._key(key),
            str(destination_path),
        )

        return destination_path
