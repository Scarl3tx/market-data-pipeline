
from datetime import datetime, timedelta

import polars as pl

from market_data.pipelines.twelvedata.silver.transform import (
    transform_stock_silver,
)


def create_stock_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "open": [100.0, 110.0],
        "high": [105.0, 115.0],
        "low": [95.0, 105.0],
        "close": [103.0, 113.0],
        "volume": [1000, 2000],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_transform_adds_date_columns():
    df = create_stock_df()

    result = transform_stock_silver(df)

    assert result.columns == [
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

    assert result["year"].to_list() == [2026, 2026]
    assert result["month"].to_list() == [11, 11]
    assert result["day"].to_list() == [5, 6]


def test_transform_preserves_ohlcv_values():
    df = create_stock_df()

    result = transform_stock_silver(df)

    assert result["open"].to_list() == [100.0, 110.0]
    assert result["high"].to_list() == [105.0, 115.0]
    assert result["low"].to_list() == [95.0, 105.0]
    assert result["close"].to_list() == [103.0, 113.0]
    assert result["volume"].to_list() == [1000, 2000]


def test_transform_calculates_iso_weekday():
    df = create_stock_df()

    result = transform_stock_silver(df)

    assert result["day_of_week"].to_list() == [4, 5]


def test_transform_sorts_by_date():
    df = create_stock_df().reverse()

    result = transform_stock_silver(df)

    assert result["date"].is_sorted()
    assert result["close"].to_list() == [103.0, 113.0]


def test_empty_dataframe_returns_empty():
    result = transform_stock_silver(pl.DataFrame())

    assert result.is_empty()


def test_indicators_are_null_with_insufficient_history():
    df = create_stock_df()

    result = transform_stock_silver(df)

    assert result["rsi_14"].null_count() == 2
    assert result["sma_50"].null_count() == 2
    assert result["sma_200"].null_count() == 2


def test_sma50_calculates_after_50_observations():
    dates = [
        datetime(2026, 1, 1) + timedelta(days=index)
        for index in range(50)
    ]

    df = pl.DataFrame({
        "asset_name": ["NVDA"] * 50,
        "date": dates,
        "open": [100.0] * 50,
        "high": [105.0] * 50,
        "low": [95.0] * 50,
        "close": [100.0] * 50,
        "volume": [1000] * 50,
    })

    result = transform_stock_silver(df)

    assert result["sma_50"].null_count() == 49
    assert result["sma_50"][-1] == 100.0
    assert result["sma_200"].null_count() == 50


def test_sma200_calculates_after_200_observations():
    dates = [
        datetime(2026, 1, 1) + timedelta(days=index)
        for index in range(200)
    ]

    df = pl.DataFrame({
        "asset_name": ["NVDA"] * 200,
        "date": dates,
        "open": [100.0] * 200,
        "high": [105.0] * 200,
        "low": [95.0] * 200,
        "close": [100.0] * 200,
        "volume": [1000] * 200,
    })

    result = transform_stock_silver(df)

    assert result["sma_200"].null_count() == 199
    assert result["sma_200"][-1] == 100.0


def test_rsi_is_50_for_flat_prices():
    dates = [
        datetime(2026, 1, 1) + timedelta(days=index)
        for index in range(20)
    ]

    df = pl.DataFrame({
        "asset_name": ["NVDA"] * 20,
        "date": dates,
        "open": [100.0] * 20,
        "high": [105.0] * 20,
        "low": [95.0] * 20,
        "close": [100.0] * 20,
        "volume": [1000] * 20,
    })

    result = transform_stock_silver(df)

    assert result["rsi_14"][13] is None
    assert result["rsi_14"][14] == 50.0


def test_rsi_is_100_when_prices_only_increase():
    dates = [
        datetime(2026, 1, 1) + timedelta(days=index)
        for index in range(20)
    ]

    closes = [100.0 + index for index in range(20)]

    df = pl.DataFrame({
        "asset_name": ["NVDA"] * 20,
        "date": dates,
        "open": closes,
        "high": [value + 2 for value in closes],
        "low": [value - 2 for value in closes],
        "close": closes,
        "volume": [1000] * 20,
    })

    result = transform_stock_silver(df)

    assert result["rsi_14"][14] == 100.0


def test_rsi_is_0_when_prices_only_decrease():
    dates = [
        datetime(2026, 1, 1) + timedelta(days=index)
        for index in range(20)
    ]

    closes = [120.0 - index for index in range(20)]

    df = pl.DataFrame({
        "asset_name": ["NVDA"] * 20,
        "date": dates,
        "open": closes,
        "high": [value + 2 for value in closes],
        "low": [value - 2 for value in closes],
        "close": closes,
        "volume": [1000] * 20,
    })

    result = transform_stock_silver(df)

    assert result["rsi_14"][14] == 0.0
