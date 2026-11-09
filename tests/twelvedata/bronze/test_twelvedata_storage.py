
from datetime import datetime

import polars as pl
import pytest

from market_data.storage.local import LocalStorage

from market_data.pipelines.twelvedata.bronze.storage import (
    get_oldest_stock_date,
    get_stock_output_path,
    save_stock_batch,
    get_latest_stock_date,
)


def sample_stock_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "open": [100.0, 105.0],
        "high": [110.0, 115.0],
        "low": [95.0, 100.0],
        "close": [105.0, 110.0],
        "volume": [1000, 1200],
    })


def test_get_stock_output_path(tmp_path):
    path = get_stock_output_path(
        bronze_path=tmp_path,
        symbol="NVDA",
        earliest_date="20261105",
        latest_date="20261106",
    )

    assert path == (
        tmp_path
        / "stocks"
        / "NVDA"
        / "NVDA_20261105_20261106_raw.pq"
    )


def test_save_stock_batch_creates_parquet(tmp_path):
    df = sample_stock_df()

    output_path = (
        tmp_path
        / "stocks"
        / "NVDA"
        / "NVDA_test_raw.pq"
    )

    saved_path = save_stock_batch(
        df=df,
        output_path=output_path,
    )

    assert saved_path.exists()

    loaded_df = pl.read_parquet(saved_path)

    assert loaded_df.equals(df)


def test_save_stock_batch_does_not_overwrite(tmp_path):
    output_path = (
        tmp_path
        / "stocks"
        / "NVDA"
        / "NVDA_test_raw.pq"
    )

    output_path.parent.mkdir(parents=True)

    original_df = pl.DataFrame({
        "original": [123]
    })

    original_df.write_parquet(output_path)

    with pytest.raises(FileExistsError):
        save_stock_batch(
            df=sample_stock_df(),
            output_path=output_path,
        )

    loaded_df = pl.read_parquet(output_path)

    assert loaded_df.equals(original_df)


def test_get_latest_stock_date(tmp_path):
    stock_path = tmp_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    df1 = pl.DataFrame({
        "date": [
            datetime(2026, 11, 1),
            datetime(2026, 11, 2),
        ]
    })

    df2 = pl.DataFrame({
        "date": [
            datetime(2026, 11, 3),
            datetime(2026, 11, 6),
        ]
    })

    df1.write_parquet(
        stock_path / "NVDA_20261101_20261102_raw.pq"
    )

    df2.write_parquet(
        stock_path / "NVDA_20261103_20261106_raw.pq"
    )

    latest_date = get_latest_stock_date(
        tmp_path,
        "NVDA",
    )

    assert latest_date.isoformat() == "2026-11-06"


def test_get_latest_stock_date_returns_none_when_missing(tmp_path):
    latest_date = get_latest_stock_date(
        tmp_path,
        "NVDA",
    )

    assert latest_date is None


def test_get_oldest_stock_date(tmp_path):
    stock_path = tmp_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    df1 = pl.DataFrame({
        "date": [
            datetime(2026, 11, 1),
            datetime(2026, 11, 2),
        ]
    })

    df2 = pl.DataFrame({
        "date": [
            datetime(2026, 11, 3),
            datetime(2026, 11, 6),
        ]
    })

    df1.write_parquet(
        stock_path / "NVDA_20261101_20261102_raw.pq"
    )

    df2.write_parquet(
        stock_path / "NVDA_20261103_20261106_raw.pq"
    )

    oldest_date = get_oldest_stock_date(
        tmp_path,
        "NVDA",
    )

    assert oldest_date.isoformat() == "2026-11-01"


def test_get_oldest_stock_date_returns_none_when_missing(tmp_path):
    oldest_date = get_oldest_stock_date(
        tmp_path,
        "NVDA",
    )

    assert oldest_date is None


def test_get_latest_stock_date_with_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261101_20261102_raw.pq",
        pl.DataFrame({
            "date": [
                datetime(2026, 11, 1),
                datetime(2026, 11, 2),
            ]
        }),
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261103_20261106_raw.pq",
        pl.DataFrame({
            "date": [
                datetime(2026, 11, 3),
                datetime(2026, 11, 6),
            ]
        }),
    )

    latest_date = get_latest_stock_date(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    )

    assert latest_date.isoformat() == "2026-11-06"


def test_get_oldest_stock_date_with_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261101_20261102_raw.pq",
        pl.DataFrame({
            "date": [
                datetime(2026, 11, 1),
                datetime(2026, 11, 2),
            ]
        }),
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261103_20261106_raw.pq",
        pl.DataFrame({
            "date": [
                datetime(2026, 11, 3),
                datetime(2026, 11, 6),
            ]
        }),
    )

    oldest_date = get_oldest_stock_date(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    )

    assert oldest_date.isoformat() == "2026-11-01"


def test_storage_date_discovery_returns_none_when_missing(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    assert get_latest_stock_date(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    ) is None

    assert get_oldest_stock_date(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    ) is None
