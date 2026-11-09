
from datetime import date, datetime

import polars as pl
import pytest

from market_data.pipelines.twelvedata.gold.validation import (
    validate_stock_data,
)


TODAY = date(2026, 11, 7)


def make_stock(**overrides) -> dict:
    stock = {
        "asset_name": "NVDA",
        "date": datetime(2026, 11, 6),
        "open": 180.0,
        "high": 185.0,
        "low": 178.0,
        "close": 183.0,
        "volume": 1000000,
    }

    stock.update(overrides)
    return stock


def make_dataframe(*stocks) -> pl.DataFrame:
    return pl.DataFrame(list(stocks))


def test_valid_stock_data_is_accepted():
    df = make_dataframe(make_stock())

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 1
    assert result["invalid"].height == 0
    assert result["rejected_rows"] == 0
    assert result["reasons"] == {}


def test_zero_volume_is_allowed():
    df = make_dataframe(make_stock(volume=0))

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 1
    assert result["rejected_rows"] == 0


@pytest.mark.parametrize(
    "column,value",
    [
        ("open", 0),
        ("open", -1),
        ("high", 0),
        ("low", -1),
        ("close", 0),
        ("close", -10),
    ],
)
def test_non_positive_prices_are_rejected(column, value):
    df = make_dataframe(make_stock(**{column: value}))

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 0
    assert result["invalid"].height == 1
    assert result["rejected_rows"] == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"high": 179.0},
        {"high": 182.0},
        {"low": 184.0},
        {"low": 181.0},
    ],
)
def test_invalid_ohlc_relationships_are_rejected(overrides):
    df = make_dataframe(make_stock(**overrides))

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 0
    assert result["invalid"].height == 1
    assert result["reasons"]["invalid_ohlc_relationship"] == 1


@pytest.mark.parametrize(
    "volume",
    [
        -1,
        1.5,
        float("nan"),
        float("inf"),
    ],
)
def test_invalid_volume_is_rejected(volume):
    df = make_dataframe(make_stock(volume=volume))

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 0
    assert result["invalid"].height == 1
    assert result["reasons"]["invalid_volume"] == 1


def test_future_date_is_rejected():
    df = make_dataframe(
        make_stock(date=datetime(2026, 11, 8))
    )

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 0
    assert result["invalid"].height == 1
    assert result["reasons"]["invalid_date"] == 1


def test_empty_symbol_is_rejected():
    df = make_dataframe(make_stock(asset_name="   "))

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 0
    assert result["reasons"]["invalid_symbol"] == 1


def test_multiple_validation_errors_are_recorded():
    df = make_dataframe(
        make_stock(open=-1, volume=-10)
    )

    result = validate_stock_data(df, today=TODAY)

    assert result["invalid"].height == 1

    errors = result["invalid"].item(
        0,
        "validation_errors",
    )

    assert "invalid_open" in errors
    assert "invalid_volume" in errors

    assert result["reasons"]["invalid_open"] == 1
    assert result["reasons"]["invalid_volume"] == 1


def test_valid_and_invalid_rows_are_separated():
    df = make_dataframe(
        make_stock(),
        make_stock(open=-1),
        make_stock(close=0),
    )

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].height == 1
    assert result["invalid"].height == 2
    assert result["rejected_rows"] == 2


def test_input_dataframe_is_not_modified():
    df = make_dataframe(
        make_stock(),
        make_stock(open=-1),
    )

    original_columns = df.columns
    original_data = df.to_dicts()

    validate_stock_data(df, today=TODAY)

    assert df.columns == original_columns
    assert df.to_dicts() == original_data


def test_missing_required_column_raises_error():
    df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 6)],
        "open": [180.0],
    })

    with pytest.raises(
        ValueError,
        match="Missing required stock columns",
    ):
        validate_stock_data(df, today=TODAY)


def test_empty_dataframe_returns_empty_results():
    df = pl.DataFrame(
        schema={
            "asset_name": pl.String,
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        }
    )

    result = validate_stock_data(df, today=TODAY)

    assert result["valid"].is_empty()
    assert result["invalid"].is_empty()
    assert result["rejected_rows"] == 0
    assert result["reasons"] == {}
