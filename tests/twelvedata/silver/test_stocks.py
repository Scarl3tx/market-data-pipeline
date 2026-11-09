
from datetime import datetime
from market_data.storage.local import LocalStorage
import polars as pl

from market_data.pipelines.twelvedata.silver.stocks import (
    run_stock_silver,
)


def make_bronze_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "open": [100.0, 102.0],
        "high": [105.0, 107.0],
        "low": [98.0, 100.0],
        "close": [103.0, 106.0],
        "volume": [1000, 1200],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_run_stock_silver_success(tmp_path, monkeypatch):
    from market_data.pipelines.twelvedata.silver import stocks

    monkeypatch.setattr(
        stocks,
        "load_stock_bronze_data",
        lambda **kwargs: make_bronze_df(),
    )

    result = run_stock_silver(
        symbol="NVDA",
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver",
    )

    assert result["success"] is True
    assert result["skipped"] is False
    assert result["rows"] == 2


def test_run_stock_silver_saves_parquet(tmp_path, monkeypatch):
    from market_data.pipelines.twelvedata.silver import stocks

    monkeypatch.setattr(
        stocks,
        "load_stock_bronze_data",
        lambda **kwargs: make_bronze_df(),
    )

    result = run_stock_silver(
        symbol="NVDA",
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver",
    )

    saved_df = pl.read_parquet(result["output_path"])

    assert saved_df.height == 2
    assert saved_df.columns == [
        "asset_name",
        "date",
        "year",
        "month",
        "day",
        "day_of_week",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "rsi_14",
        "sma_50",
        "sma_200",
    ]

    assert saved_df["rsi_14"].null_count() == 2
    assert saved_df["sma_50"].null_count() == 2
    assert saved_df["sma_200"].null_count() == 2


def test_run_stock_silver_skips_when_no_bronze_data(tmp_path, monkeypatch):
    from market_data.pipelines.twelvedata.silver import stocks

    monkeypatch.setattr(
        stocks,
        "load_stock_bronze_data",
        lambda **kwargs: pl.DataFrame(),
    )

    result = run_stock_silver(
        symbol="NVDA",
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver",
    )

    assert result["success"] is True
    assert result["skipped"] is True
    assert result["reason"] == "No Bronze data found"


def test_run_stock_silver_returns_failure_on_error(tmp_path, monkeypatch):
    from market_data.pipelines.twelvedata.silver import stocks

    def raise_error(**kwargs):
        raise RuntimeError("Test failure")

    monkeypatch.setattr(
        stocks,
        "load_stock_bronze_data",
        raise_error,
    )

    result = run_stock_silver(
        symbol="NVDA",
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver",
    )

    assert result["success"] is False
    assert "Test failure" in result["error"]


def test_run_stock_silver_with_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    storage.write_parquet(
        "bronze/twelvedata/stocks/NVDA/"
        "NVDA_20261105_20261106_raw.pq",
        make_bronze_df(),
    )

    result = run_stock_silver(
        symbol="NVDA",
        storage=storage,
    )

    expected_key = "silver/stocks/NVDA/NVDA_silver.pq"

    assert result["success"] is True
    assert result["skipped"] is False
    assert result["output_path"] == expected_key
    assert storage.exists(expected_key)

    saved_df = storage.read_parquet(expected_key)

    assert saved_df.height == 2
    assert saved_df.columns == [
        "asset_name",
        "date",
        "year",
        "month",
        "day",
        "day_of_week",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "rsi_14",
        "sma_50",
        "sma_200",
    ]
