from datetime import datetime

import polars as pl
from market_data.storage.local import LocalStorage
from market_data.pipelines.twelvedata.silver.storage import (
    load_stock_bronze_data
)


def create_stock_df(dates, closes):
    return pl.DataFrame({
        "asset_name": ["NVDA"] * len(dates),
        "date": [
            datetime.strptime(d, "%Y-%m-%d")
            for d in dates
        ],
        "open": closes,
        "high": [c + 2 for c in closes],
        "low": [c - 2 for c in closes],
        "close": closes,
        "volume": [1000] * len(dates)
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_load_stock_bronze_data(tmp_path):
    bronze_path = tmp_path / "bronze"
    stock_path = bronze_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    df = create_stock_df(
        ["2026-11-04", "2026-11-05"],
        [100.0, 110.0]
    )

    df.write_parquet(
        stock_path / "NVDA_20261104_20261105_raw.pq"
    )

    result = load_stock_bronze_data(
        bronze_path,
        "NVDA"
    )

    assert result.height == 2
    assert result["asset_name"][0] == "NVDA"
    assert result["close"].to_list() == [100.0, 110.0]


def test_missing_stock_folder_returns_empty_dataframe(tmp_path):
    result = load_stock_bronze_data(
        tmp_path,
        "NVDA"
    )

    assert result.is_empty()


def test_no_parquet_files_returns_empty_dataframe(tmp_path):
    stock_path = tmp_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    result = load_stock_bronze_data(
        tmp_path,
        "NVDA"
    )

    assert result.is_empty()

def test_multiple_batches_are_combined(tmp_path):
    stock_path = tmp_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    df1 = create_stock_df(
        ["2026-11-03", "2026-11-04"],
        [100.0, 110.0]
    )

    df2 = create_stock_df(
        ["2026-11-05", "2026-11-06"],
        [120.0, 130.0]
    )

    df1.write_parquet(
        stock_path / "NVDA_20261103_20261104_raw.pq"
    )

    df2.write_parquet(
        stock_path / "NVDA_20261105_20261106_raw.pq"
    )

    result = load_stock_bronze_data(tmp_path, "NVDA")

    assert result.height == 4
    assert result["close"].to_list() == [
        100.0, 110.0, 120.0, 130.0
    ]

def test_duplicate_dates_keep_last_record(tmp_path):
    stock_path = tmp_path / "stocks" / "NVDA"
    stock_path.mkdir(parents=True)

    df1 = create_stock_df(["2026-11-04"], [100.0])
    df2 = create_stock_df(["2026-11-04"], [150.0])

    df1.write_parquet(
        stock_path / "NVDA_20261104_20261104_raw.pq"
    )

    df2.write_parquet(
        stock_path / "NVDA_20261104_20261104_second_raw.pq"
    )

    result = load_stock_bronze_data(tmp_path, "NVDA")

    assert result.height == 1
    assert result["close"][0] == 150.0

def test_load_stock_bronze_data_with_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    df1 = create_stock_df(
        ["2026-11-03", "2026-11-04"],
        [100.0, 110.0],
    )

    df2 = create_stock_df(
        ["2026-11-05", "2026-11-06"],
        [120.0, 130.0],
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261103_20261104_raw.pq",
        df1,
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261105_20261106_raw.pq",
        df2,
    )

    result = load_stock_bronze_data(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    )

    assert result.height == 4
    assert result["close"].to_list() == [
        100.0, 110.0, 120.0, 130.0,
    ]


def test_storage_duplicate_dates_keep_last_record(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    df1 = create_stock_df(
        ["2026-11-04"],
        [100.0],
    )

    df2 = create_stock_df(
        ["2026-11-04"],
        [150.0],
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261103_20261104_raw.pq",
        df1,
    )

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/NVDA_20261103_20261104_second_raw.pq",
        df2,
    )

    result = load_stock_bronze_data(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    )

    assert result.height == 1
    assert result["close"][0] == 150.0


def test_storage_missing_symbol_returns_empty_dataframe(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    result = load_stock_bronze_data(
        bronze_path=tmp_path,
        symbol="NVDA",
        storage=storage,
    )

    assert result.is_empty()
