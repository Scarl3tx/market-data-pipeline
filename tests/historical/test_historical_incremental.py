
from datetime import date, timedelta
from unittest.mock import Mock

import polars as pl
import pytest

from src.historical.ingestion.stocks import update_historical_stock


@pytest.fixture
def historical_data_path(tmp_path, monkeypatch):
    """
    Redirect historical Bronze storage to a temporary directory.
    """
    path = tmp_path / "historical" / "stocks"

    monkeypatch.setattr(
        "src.historical.ingestion.stocks.HISTORICAL_STOCKS_DATA_PATH",
        path
    )

    return path


def create_bronze_file(path, symbol, days_ago):
    stock_path = path / symbol
    stock_path.mkdir(parents=True, exist_ok=True)

    latest_date = date.today() - timedelta(days=days_ago)

    df = pl.DataFrame({
        "date": [latest_date]
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )

    output_path = stock_path / f"{symbol}_test_raw.pq"
    df.write_parquet(output_path)


def test_skip_update_within_30_days(historical_data_path):
    create_bronze_file(historical_data_path, "NVDA", 10)

    provider = Mock()

    result = update_historical_stock(
        provider=provider,
        symbol="NVDA",
        buffer_days=30
    )

    assert result["success"] is True
    assert result["updated"] is False

    provider.get_stock_history.assert_not_called()


def test_skip_update_at_exactly_30_days(historical_data_path):
    create_bronze_file(historical_data_path, "NVDA", 30)

    provider = Mock()

    result = update_historical_stock(
        provider=provider,
        symbol="NVDA",
        buffer_days=30
    )

    assert result["success"] is True
    assert result["updated"] is False

    provider.get_stock_history.assert_not_called()


def test_update_when_gap_exceeds_30_days(
    historical_data_path,
    monkeypatch
):
    create_bronze_file(historical_data_path, "NVDA", 31)

    provider = Mock()

    expected_start = (
        date.today() - timedelta(days=30)
    ).isoformat()

    expected_end = date.today().isoformat()

    # Mock the existing ingestion function so no API call or
    # actual Parquet write is performed.
    ingestion_mock = Mock(return_value={
        "symbol": "NVDA",
        "success": True,
        "rows": 5,
        "earliest_date": "20261004",
        "latest_date": "20261008",
        "has_data": True
    })

    monkeypatch.setattr(
        "src.historical.ingestion.stocks.ingest_historical_stock_batch",
        ingestion_mock
    )

    result = update_historical_stock(
        provider=provider,
        symbol="NVDA",
        buffer_days=30
    )

    
    print("RESULT:", result)
    print("MOCK CALLED:", ingestion_mock.call_args)

    assert result["success"] is True
    assert result["updated"] is True

    ingestion_mock.assert_called_once_with(
        provider=provider,
        symbol="NVDA",
        start_date=expected_start,
        end_date=expected_end
    )


def test_skip_update_when_latest_date_is_today(historical_data_path):
    create_bronze_file(historical_data_path, "NVDA", 0)

    provider = Mock()

    result = update_historical_stock(
        provider=provider,
        symbol="NVDA",
        buffer_days=30
    )

    assert result["success"] is True
    assert result["updated"] is False

    provider.get_stock_history.assert_not_called()


def test_no_historical_files_returns_failure(historical_data_path):
    provider = Mock()

    result = update_historical_stock(
        provider=provider,
        symbol="NVDA",
        buffer_days=30
    )

    assert result["success"] is False
    assert result["updated"] is False

    provider.get_stock_history.assert_not_called()
