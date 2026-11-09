
import json
from pathlib import Path

import polars as pl
import pytest

from market_data.storage.local import LocalStorage


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "storage")


def test_initializes_root(storage):
    assert storage.root.is_dir()


def test_write_and_read_bytes(storage):
    key = "bronze/twelvedata/NVDA/raw.pq"
    content = b"test data"

    assert storage.write_bytes(key, content) == key
    assert storage.read_bytes(key) == content
    assert storage.exists(key)


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


def test_delete_existing_file(storage):
    key = "bronze/test.bin"
    storage.write_bytes(key, b"data")

    assert storage.delete(key) is True
    assert storage.exists(key) is False


def test_delete_missing_file(storage):
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


@pytest.mark.parametrize(
    "key",
    [
        "../outside.txt",
        "/absolute/path.txt",
        "bronze/../../outside.txt",
        "bronze\\outside.txt",
    ],
)
def test_rejects_invalid_keys(storage, key):
    with pytest.raises(ValueError):
        storage.exists(key)


def test_rejects_invalid_prefix(storage):
    with pytest.raises(ValueError):
        storage.list_files("../outside")


def test_rejects_non_bytes_data(storage):
    with pytest.raises(TypeError):
        storage.write_bytes("test.txt", "not bytes")
