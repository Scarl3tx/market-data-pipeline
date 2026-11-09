
from pathlib import Path

import polars as pl
import pytest

from moto import mock_aws
import boto3

from market_data.storage.s3 import S3Storage


@pytest.fixture
def storage():
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="ap-southeast-1",
        )

        client.create_bucket(
            Bucket="market-data-test-bucket",
            CreateBucketConfiguration={
                "LocationConstraint": "ap-southeast-1",
            },
        )

        yield S3Storage(
            bucket="market-data-test-bucket",
            client=client,
        )


def test_write_and_read_bytes(storage):
    key = "bronze/twelvedata/NVDA/raw.pq"

    storage.write_bytes(key, b"market data")

    assert storage.exists(key)
    assert storage.read_bytes(key) == b"market data"


def test_write_bytes_does_not_overwrite(storage):
    key = "bronze/test.bin"

    storage.write_bytes(key, b"original")

    with pytest.raises(FileExistsError):
        storage.write_bytes(
            key,
            b"replacement",
            overwrite=False,
        )

    assert storage.read_bytes(key) == b"original"


def test_write_bytes_overwrites_when_enabled(storage):
    key = "bronze/test.bin"

    storage.write_bytes(key, b"original")
    storage.write_bytes(key, b"replacement")

    assert storage.read_bytes(key) == b"replacement"


def test_list_files_returns_sorted_relative_keys(storage):
    storage.write_bytes("bronze/NVDA/b.pq", b"b")
    storage.write_bytes("bronze/NVDA/a.pq", b"a")
    storage.write_bytes("bronze/AAPL/a.pq", b"a")

    result = storage.list_files(
        prefix="bronze/NVDA",
        pattern="*.pq",
    )

    assert result == [
        "bronze/NVDA/a.pq",
        "bronze/NVDA/b.pq",
    ]


def test_list_files_returns_empty_for_missing_prefix(storage):
    assert storage.list_files("bronze/MISSING") == []


def test_json_round_trip(storage):
    key = "state/checkpoint.json"

    data = {
        "NVDA": {
            "recent_complete": True,
            "backfill_complete": False,
        }
    }

    storage.write_json(key, data)

    assert storage.read_json(key) == data


def test_parquet_round_trip(storage):
    key = "silver/stocks/NVDA/data.pq"

    df = pl.DataFrame({
        "symbol": ["NVDA", "NVDA"],
        "close": [100.0, 110.0],
    })

    storage.write_parquet(key, df)

    result = storage.read_parquet(key)

    assert result.equals(df)


def test_delete_existing_object(storage):
    key = "bronze/test.bin"

    storage.write_bytes(key, b"data")

    assert storage.delete(key) is True
    assert storage.exists(key) is False


def test_delete_missing_object(storage):
    assert storage.delete("missing.bin") is False


def test_upload_and_download_file(storage, tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("market data", encoding="utf-8")

    storage.upload_file(source, "bronze/source.txt")

    destination = tmp_path / "downloads" / "copy.txt"

    result = storage.download_file(
        "bronze/source.txt",
        destination,
    )

    assert result == destination
    assert destination.read_text(encoding="utf-8") == "market data"


def test_prefix_is_applied_to_all_operations():
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="ap-southeast-1",
        )

        client.create_bucket(
            Bucket="market-data-prefix-test",
            CreateBucketConfiguration={
                "LocationConstraint": "ap-southeast-1",
            },
        )

        storage = S3Storage(
            bucket="market-data-prefix-test",
            prefix="production",
            client=client,
        )

        storage.write_bytes("bronze/test.bin", b"data")

        assert client.head_object(
            Bucket="market-data-prefix-test",
            Key="production/bronze/test.bin",
        )

        assert storage.read_bytes("bronze/test.bin") == b"data"
        assert storage.list_files("bronze") == [
            "bronze/test.bin"
        ]


def test_rejects_invalid_keys(storage):
    with pytest.raises(ValueError):
        storage.exists("../outside.txt")


def test_rejects_empty_bucket():
    with pytest.raises(ValueError):
        S3Storage(bucket="")
