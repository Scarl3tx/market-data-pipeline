
from datetime import datetime

import polars as pl
import pytest

from market_data.storage.local import LocalStorage
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
    TwelveDataAPIError,
)
from market_data.pipelines.twelvedata.bronze.stocks import (
    ingest_stock_batch,
)


class MockProvider:

    def __init__(self, df=None, error=None):
        self.df = df
        self.error = error

    def get_stock_history(self, symbol, start_date, end_date):
        if self.error:
            raise self.error

        return self.df


@pytest.fixture
def stock_df():
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
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_successful_ingestion(stock_df, tmp_path):
    provider = MockProvider(df=stock_df)

    result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        bronze_path=tmp_path,
    )

    expected_path = (
        tmp_path
        / "stocks"
        / "NVDA"
        / "NVDA_20261105_20261106_raw.pq"
    )

    assert result["success"] is True
    assert result["has_data"] is True
    assert result["rows"] == 2
    assert result["skipped"] is False
    assert result["output_path"] == expected_path
    assert expected_path.exists()


def test_empty_response_is_successful(tmp_path):
    provider = MockProvider(df=pl.DataFrame())

    result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        bronze_path=tmp_path,
    )

    assert result["success"] is True
    assert result["has_data"] is False
    assert result["rows"] == 0


def test_api_failure_returns_unsuccessful_result(tmp_path):
    provider = MockProvider(
        error=TwelveDataAPIError("Invalid API key")
    )

    result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        bronze_path=tmp_path,
    )

    assert result["success"] is False
    assert result["has_data"] is False
    assert "Invalid API key" in result["error"]


def test_daily_limit_propagates(tmp_path):
    provider = MockProvider(
        error=TwelveDataDailyLimitError("Daily limit reached")
    )

    with pytest.raises(TwelveDataDailyLimitError):
        ingest_stock_batch(
            provider=provider,
            symbol="NVDA",
            start_date="2026-11-05",
            end_date="2026-11-06",
            bronze_path=tmp_path,
        )


def test_existing_batch_is_skipped(stock_df, tmp_path):
    provider = MockProvider(df=stock_df)

    first_result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        bronze_path=tmp_path,
    )

    second_result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        bronze_path=tmp_path,
    )

    assert first_result["skipped"] is False
    assert second_result["skipped"] is True
    assert second_result["success"] is True


def test_invalid_dataframe_is_not_saved(tmp_path):
    invalid_df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 5)],
        "open": [100.0],
        "high": [90.0],
        "low": [95.0],
        "close": [105.0],
        "volume": [1000],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )

    provider = MockProvider(df=invalid_df)

    with pytest.raises(ValueError, match="Invalid OHLC"):
        ingest_stock_batch(
            provider=provider,
            symbol="NVDA",
            start_date="2026-11-05",
            end_date="2026-11-05",
            bronze_path=tmp_path,
        )

    assert not list(tmp_path.rglob("*.pq"))


def test_ingestion_with_storage_backend(stock_df, tmp_path):
    provider = MockProvider(df=stock_df)
    storage = LocalStorage(tmp_path / "storage")

    result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        storage=storage,
    )

    expected_key = (
        "bronze/twelvedata/stocks/NVDA/"
        "NVDA_20261105_20261106_raw.pq"
    )

    assert result["success"] is True
    assert result["skipped"] is False
    assert result["output_path"] == expected_key
    assert storage.exists(expected_key)

    saved_df = storage.read_parquet(expected_key)
    assert saved_df.equals(stock_df)


def test_existing_storage_batch_is_skipped(stock_df, tmp_path):
    provider = MockProvider(df=stock_df)
    storage = LocalStorage(tmp_path / "storage")

    first_result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        storage=storage,
    )

    second_result = ingest_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-11-05",
        end_date="2026-11-06",
        storage=storage,
    )

    assert first_result["skipped"] is False
    assert second_result["skipped"] is True
    assert second_result["output_path"] == first_result["output_path"]


def test_invalid_dataframe_is_not_saved_to_storage(tmp_path):
    invalid_df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 5)],
        "open": [100.0],
        "high": [90.0],
        "low": [95.0],
        "close": [105.0],
        "volume": [1000],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )

    provider = MockProvider(df=invalid_df)
    storage = LocalStorage(tmp_path / "storage")

    with pytest.raises(ValueError, match="Invalid OHLC"):
        ingest_stock_batch(
            provider=provider,
            symbol="NVDA",
            start_date="2026-11-05",
            end_date="2026-11-05",
            storage=storage,
        )

    assert storage.list_files() == []
