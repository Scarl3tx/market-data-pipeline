
import json

import pytest

from market_data.pipelines.alphavantage.bronze.config import (
    load_news_tickers,
)


def test_load_news_tickers_returns_enabled_stocks(tmp_path, monkeypatch):
    config_path = tmp_path / "stocks.json"

    config_path.write_text(
        json.dumps({
            "stocks": [
                {"symbol": "NVDA", "enabled": True},
                {"symbol": "AMD", "enabled": False},
                {"symbol": "AAPL", "enabled": True},
            ]
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    assert load_news_tickers() == ["NVDA", "AAPL"]


def test_load_news_tickers_returns_empty_list_when_none_enabled(
    tmp_path, monkeypatch
):
    config_path = tmp_path / "stocks.json"

    config_path.write_text(
        json.dumps({
            "stocks": [
                {"symbol": "NVDA", "enabled": False}
            ]
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    assert load_news_tickers() == []


def test_load_news_tickers_rejects_missing_stocks(tmp_path, monkeypatch):
    config_path = tmp_path / "stocks.json"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    with pytest.raises(ValueError):
        load_news_tickers()


def test_load_news_tickers_rejects_invalid_symbol(tmp_path, monkeypatch):
    config_path = tmp_path / "stocks.json"

    config_path.write_text(
        json.dumps({
            "stocks": [
                {"symbol": "", "enabled": True}
            ]
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    with pytest.raises(ValueError):
        load_news_tickers()

import json

from market_data.pipelines.alphavantage.bronze.config import (
    load_news_start_time,
)
from market_data.paths import STOCKS_CONFIG_PATH


def test_load_news_start_time_uses_earliest_enabled_stock(
    tmp_path,
    monkeypatch,
):
    config = {
        "stocks": [
            {
                "symbol": "NVDA",
                "enabled": True,
                "start_date": "2000-01-01",
            },
            {
                "symbol": "AMD",
                "enabled": True,
                "start_date": "2010-05-15",
            },
        ]
    }

    config_path = tmp_path / "stocks_config.json"

    config_path.write_text(
        json.dumps(config),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    assert load_news_start_time() == "20000101T0000"


def test_load_news_start_time_ignores_disabled_stocks(
    tmp_path,
    monkeypatch,
):
    config = {
        "stocks": [
            {
                "symbol": "NVDA",
                "enabled": True,
                "start_date": "2010-01-01",
            },
            {
                "symbol": "AMD",
                "enabled": False,
                "start_date": "2000-01-01",
            },
        ]
    }

    config_path = tmp_path / "stocks_config.json"

    config_path.write_text(
        json.dumps(config),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    assert load_news_start_time() == "20100101T0000"


def test_load_news_start_time_returns_none_when_no_stocks_enabled(
    tmp_path,
    monkeypatch,
):
    config = {
        "stocks": [
            {
                "symbol": "NVDA",
                "enabled": False,
                "start_date": "2000-01-01",
            }
        ]
    }

    config_path = tmp_path / "stocks_config.json"

    config_path.write_text(
        json.dumps(config),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    assert load_news_start_time() is None


def test_load_news_start_time_rejects_invalid_date(
    tmp_path,
    monkeypatch,
):
    config = {
        "stocks": [
            {
                "symbol": "NVDA",
                "enabled": True,
                "start_date": "invalid",
            }
        ]
    }

    config_path = tmp_path / "stocks_config.json"

    config_path.write_text(
        json.dumps(config),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    import pytest

    with pytest.raises(ValueError, match="Invalid stock start_date"):
        load_news_start_time()
