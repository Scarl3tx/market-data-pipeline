
from datetime import datetime

import polars as pl
import pytest

from market_data.pipelines.twelvedata.silver.validation import (
    validate_stock_silver,
)


@pytest.fixture
def valid_silver_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "year": [2026, 2026],
        "month": [11, 11],
        "day": [5, 6],
        "day_of_week": [4, 5],
        "open": [100.0, 110.0],
        "high": [105.0, 115.0],
        "low": [95.0, 105.0],
        "close": [103.0, 113.0],
        "volume": [1000, 2000],
        "rsi_14": [None, None],
        "sma_50": [None, None],
        "sma_200": [None, None],
    }).with_columns([
        pl.col("date").cast(pl.Datetime("ns")),
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int8),
        pl.col("day").cast(pl.Int8),
        pl.col("day_of_week").cast(pl.Int8),
        pl.col("rsi_14").cast(pl.Float64),
        pl.col("sma_50").cast(pl.Float64),
        pl.col("sma_200").cast(pl.Float64),
    ])


def test_valid_silver_dataframe_passes(valid_silver_df):
    assert validate_stock_silver(valid_silver_df) is True


def test_incorrect_columns_raise_error(valid_silver_df):
    df = valid_silver_df.drop("volume")

    with pytest.raises(ValueError, match="column mismatch"):
        validate_stock_silver(df)


def test_incorrect_dtype_raises_error(valid_silver_df):
    df = valid_silver_df.with_columns(
        pl.col("close").cast(pl.String)
    )

    with pytest.raises(TypeError, match="Invalid type"):
        validate_stock_silver(df)


def test_duplicate_dates_raise_error(valid_silver_df):
    df = valid_silver_df.with_columns(
        pl.Series(
            "date",
            [
                datetime(2026, 11, 5),
                datetime(2026, 11, 5),
            ],
            dtype=pl.Datetime("ns"),
        )
    )

    with pytest.raises(ValueError, match="Duplicate dates"):
        validate_stock_silver(df)


def test_unsorted_dates_raise_error(valid_silver_df):
    df = valid_silver_df.reverse()

    with pytest.raises(ValueError, match="must be sorted"):
        validate_stock_silver(df)


def test_null_required_values_raise_error(valid_silver_df):
    df = valid_silver_df.with_columns(
        pl.Series(
            "close",
            [103.0, None],
            dtype=pl.Float64,
        )
    )

    with pytest.raises(ValueError, match="Null values"):
        validate_stock_silver(df)


def test_null_indicator_values_are_allowed(valid_silver_df):
    assert validate_stock_silver(valid_silver_df) is True


def test_populated_indicator_values_pass(valid_silver_df):
    df = valid_silver_df.with_columns([
        pl.Series("rsi_14", [55.0, 60.0], dtype=pl.Float64),
        pl.Series("sma_50", [100.0, 101.0], dtype=pl.Float64),
        pl.Series("sma_200", [95.0, 96.0], dtype=pl.Float64),
    ])

    assert validate_stock_silver(df) is True


def test_invalid_rsi_raises_error(valid_silver_df):
    df = valid_silver_df.with_columns(
        pl.Series("rsi_14", [50.0, 105.0], dtype=pl.Float64)
    )

    with pytest.raises(ValueError, match="Invalid RSI"):
        validate_stock_silver(df)


def test_invalid_ohlc_raises_error(valid_silver_df):
    df = valid_silver_df.with_columns(
        pl.Series(
            "high",
            [90.0, 115.0],
            dtype=pl.Float64,
        )
    )

    with pytest.raises(ValueError, match="Invalid OHLC"):
        validate_stock_silver(df)
