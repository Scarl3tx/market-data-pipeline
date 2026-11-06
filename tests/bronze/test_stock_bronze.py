import os
from datetime import datetime, timezone

import pytest

# The stock module checks for an API key during import.
# This prevents tests from requiring a real API key.
os.environ.setdefault("ALPHA_VANTAGE_KEY", "test-key")

import src.alphavantage_trackers.pipelines.av_stocks.bronze.extract_alphavantage_stocks_data as stock_bronze

from alphavantage_trackers.api.exceptions import (
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError
)


@pytest.fixture
def stock_setup(tmp_path, monkeypatch):
    """Set up isolated stock storage and a fixed UTC timestamp."""

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(stock_bronze, "STOCKS_PATH", tmp_path)
    monkeypatch.setattr(stock_bronze, "datetime", FixedDateTime)

    return tmp_path


@pytest.fixture
def stock_configs():
    return {
        "NVDA": {"function": "TIME_SERIES_DAILY"},
        "AMD": {"function": "TIME_SERIES_DAILY"}
    }


def test_successful_stock_extraction(
    stock_setup, stock_configs, monkeypatch
):
    """Successful extraction and saving should return True."""

    monkeypatch.setattr(
        stock_bronze,
        "extract_raw_stock",
        lambda name, config: {"mock": "data"}
    )

    monkeypatch.setattr(
        stock_bronze,
        "save_raw_to_disk",
        lambda name, data: True
    )

    result = stock_bronze.run_alphavantage_stock_bronze(stock_configs)

    assert result is True


def test_empty_api_response(
    stock_setup, stock_configs, monkeypatch
):
    """An empty API response should mark the run as unsuccessful."""

    save_calls = []

    monkeypatch.setattr(
        stock_bronze,
        "extract_raw_stock",
        lambda name, config: {}
    )

    monkeypatch.setattr(
        stock_bronze,
        "save_raw_to_disk",
        lambda name, data: save_calls.append(name)
    )

    result = stock_bronze.run_alphavantage_stock_bronze(stock_configs)

    assert result is False
    assert save_calls == []


def test_local_quota_exceeded(
    stock_setup, stock_configs, monkeypatch
):
    """Local quota exhaustion should stop further extraction."""

    extraction_calls = []

    def mock_extract(name, config):
        extraction_calls.append(name)
        raise AlphaVantageQuotaExceededError(retry_after=3600)

    monkeypatch.setattr(
        stock_bronze,
        "extract_raw_stock",
        mock_extract
    )

    result = stock_bronze.run_alphavantage_stock_bronze(stock_configs)

    assert result is False
    assert extraction_calls == ["NVDA"]


def test_api_rate_limit_exceeded(
    stock_setup, stock_configs, monkeypatch
):
    """An Alpha Vantage rate-limit response should stop further extraction."""

    extraction_calls = []

    def mock_extract(name, config):
        extraction_calls.append(name)
        raise AlphaVantageRateLimitError("API rate limit reached")

    monkeypatch.setattr(
        stock_bronze,
        "extract_raw_stock",
        mock_extract
    )

    result = stock_bronze.run_alphavantage_stock_bronze(stock_configs)

    assert result is False
    assert extraction_calls == ["NVDA"]


def test_existing_hourly_file_is_skipped(
    stock_setup, stock_configs, monkeypatch
):
    """Existing hourly files should prevent unnecessary API requests."""

    timestamp = "20261104_12"

    for stock_name in stock_configs:
        stock_dir = stock_setup / stock_name
        stock_dir.mkdir(parents=True)

        existing_file = (
            stock_dir / f"{timestamp}_{stock_name}_raw.pq"
        )
        existing_file.touch()

    extraction_calls = []

    monkeypatch.setattr(
        stock_bronze,
        "extract_raw_stock",
        lambda name, config: extraction_calls.append(name)
    )

    result = stock_bronze.run_alphavantage_stock_bronze(stock_configs)

    assert result is True
    assert extraction_calls == []