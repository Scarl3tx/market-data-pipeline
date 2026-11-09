
from datetime import datetime

import polars as pl
import pytest

from market_data.utils.validation import validate_stock_dataframe


@pytest.fixture
def valid_stock_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6)
        ],
        "open": [100.0, 105.0],
        "high": [110.0, 115.0],
        "low": [95.0, 100.0],
        "close": [105.0, 110.0],
        "volume": [1000, 1200]
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_valid_stock_dataframe_passes(valid_stock_df):
    assert validate_stock_dataframe(
        valid_stock_df,
        "NVDA"
    )


def test_duplicate_dates_raise_error(valid_stock_df):
    df = valid_stock_df.with_columns(
        pl.Series(
            "date",
            [
                datetime(2026, 11, 5),
                datetime(2026, 11, 5)
            ],
            dtype=pl.Datetime("ns")
        )
    )

    with pytest.raises(ValueError, match="Duplicate dates"):
        validate_stock_dataframe(df, "NVDA")


def test_unsorted_dates_raise_error(valid_stock_df):
    df = valid_stock_df.reverse()

    with pytest.raises(ValueError, match="not sorted"):
        validate_stock_dataframe(df, "NVDA")


def test_null_values_raise_error(valid_stock_df):
    df = valid_stock_df.with_columns(
        pl.when(pl.col("date") == datetime(2026, 11, 5))
        .then(None)
        .otherwise(pl.col("close"))
        .alias("close")
    )

    with pytest.raises(ValueError, match="Null values"):
        validate_stock_dataframe(df, "NVDA")


def test_invalid_ohlc_raises_error(valid_stock_df):
    df = valid_stock_df.with_columns(
        pl.lit(90.0).alias("high")
    )

    with pytest.raises(ValueError, match="Invalid OHLC"):
        validate_stock_dataframe(df, "NVDA")
