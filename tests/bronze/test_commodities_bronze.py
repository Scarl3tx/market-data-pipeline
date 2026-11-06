import os
from datetime import datetime, timezone

import pytest

os.environ.setdefault("ALPHA_VANTAGE_KEY", "test-key")

import src.alphavantage_trackers.pipelines.av_commodities.bronze.extract_alphavantage_commodities_data as commodities_bronze

from alphavantage_trackers.api.exceptions import (
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError
)

@pytest.fixture
def commodities_setup(tmp_path, monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        commodities_bronze,
        "COMMODITIES_PATH",
        tmp_path
    )

    monkeypatch.setattr(
        commodities_bronze,
        "datetime",
        FixedDateTime
    )

    return tmp_path


@pytest.fixture
def commodities_configs():
    return {
        "XAU": {"function": "GOLD_SILVER_SPOT"},
        "XAG": {"function": "GOLD_SILVER_SPOT"}
    }

def test_successful_commodities_extraction(
    commodities_setup, commodities_configs, monkeypatch
):
    monkeypatch.setattr(
        commodities_bronze,
        "extract_raw_commodities",
        lambda name, config: {
            "nominal": name,
            "data": [{"date": "2026-11-04", "value": "3900.00"}]
        }
    )

    monkeypatch.setattr(
        commodities_bronze,
        "save_raw_to_disk",
        lambda name, data: True
    )

    result = commodities_bronze.run_alphavantage_commodities_bronze(
        commodities_configs
    )

    assert result is True

def test_empty_api_response(
    commodities_setup, commodities_configs, monkeypatch
):
    monkeypatch.setattr(
        commodities_bronze,
        "extract_raw_commodities",
        lambda name, config: {}
    )

    result = commodities_bronze.run_alphavantage_commodities_bronze(
        commodities_configs
    )

    assert result is False

def test_invalid_observation_format(
    monkeypatch
):
    monkeypatch.setattr(
        commodities_bronze.av_rate_limiter,
        "acquire",
        lambda: None
    )

    monkeypatch.setattr(
        commodities_bronze.requests,
        "get",
        lambda *args, **kwargs: type(
            "MockResponse",
            (),
            {
                "raise_for_status": lambda self: None,
                "json": lambda self: {
                    "nominal": "XAUUSD",
                    "data": "invalid"
                }
            }
        )()
    )

    result = commodities_bronze.extract_raw_commodities(
        "XAU",
        {"function": "GOLD_SILVER_SPOT"}
    )

    assert result == {}

def test_local_quota_exceeded(
    commodities_setup, commodities_configs, monkeypatch
):
    extraction_calls = []

    def mock_extract(name, config):
        extraction_calls.append(name)
        raise AlphaVantageQuotaExceededError(retry_after=3600)

    monkeypatch.setattr(
        commodities_bronze,
        "extract_raw_commodities",
        mock_extract
    )

    result = commodities_bronze.run_alphavantage_commodities_bronze(
        commodities_configs
    )

    assert result is False
    assert extraction_calls == ["XAU"]

def test_api_rate_limit_exceeded(
    commodities_setup, commodities_configs, monkeypatch
):
    extraction_calls = []

    def mock_extract(name, config):
        extraction_calls.append(name)
        raise AlphaVantageRateLimitError("API rate limit reached")

    monkeypatch.setattr(
        commodities_bronze,
        "extract_raw_commodities",
        mock_extract
    )

    result = commodities_bronze.run_alphavantage_commodities_bronze(
        commodities_configs
    )

    assert result is False
    assert extraction_calls == ["XAU"]

def test_existing_hourly_file_is_skipped(
    commodities_setup, commodities_configs, monkeypatch
):
    timestamp = "20261104_12"

    for asset_name in commodities_configs:
        asset_dir = commodities_setup / asset_name
        asset_dir.mkdir(parents=True)

        existing_file = (
            asset_dir / f"{timestamp}_{asset_name}_raw.pq"
        )
        existing_file.touch()

    extraction_calls = []

    monkeypatch.setattr(
        commodities_bronze,
        "extract_raw_commodities",
        lambda name, config: extraction_calls.append(name)
    )

    result = commodities_bronze.run_alphavantage_commodities_bronze(
        commodities_configs
    )

    assert result is True
    assert extraction_calls == []

