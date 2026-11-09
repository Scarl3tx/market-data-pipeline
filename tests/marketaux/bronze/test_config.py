
import json

import pytest

from market_data.pipelines.marketaux.bronze.config import (
    MarketauxConfigError,
    load_enabled_stocks,
)


def write_config(tmp_path, config):
    config_path = tmp_path / "stocks_config.json"

    with open(config_path, "w", encoding="utf-8") as file:
        json.dump(config, file)

    return config_path


def test_load_enabled_stocks(tmp_path):
    config_path = write_config(
        tmp_path,
        {
            "stocks": [
                {
                    "symbol": "NVDA",
                    "name": "NVIDIA",
                    "enabled": True,
                    "start_date": "2000-01-01",
                },
                {
                    "symbol": "AMD",
                    "name": "Advanced Micro Devices",
                    "enabled": False,
                    "start_date": "2000-01-01",
                },
                {
                    "symbol": "msft",
                    "name": " Microsoft ",
                    "enabled": True,
                    "start_date": "2000-01-01",
                },
            ]
        },
    )

    result = load_enabled_stocks(config_path)

    assert result == [
        {
            "symbol": "NVDA",
            "name": "NVIDIA",
        },
        {
            "symbol": "MSFT",
            "name": "Microsoft",
        },
    ]


def test_empty_stock_list(tmp_path):
    config_path = write_config(
        tmp_path,
        {"stocks": []},
    )

    assert load_enabled_stocks(config_path) == []


def test_missing_config_file(tmp_path):
    config_path = tmp_path / "missing.json"

    with pytest.raises(
        MarketauxConfigError,
        match="Stock configuration not found",
    ):
        load_enabled_stocks(config_path)


def test_invalid_json(tmp_path):
    config_path = tmp_path / "stocks_config.json"
    config_path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(
        MarketauxConfigError,
        match="Invalid JSON",
    ):
        load_enabled_stocks(config_path)


def test_missing_stocks_list(tmp_path):
    config_path = write_config(
        tmp_path,
        {"other": []},
    )

    with pytest.raises(
        MarketauxConfigError,
        match="stocks list",
    ):
        load_enabled_stocks(config_path)


def test_invalid_stock_entry(tmp_path):
    config_path = write_config(
        tmp_path,
        {"stocks": ["NVDA"]},
    )

    with pytest.raises(
        MarketauxConfigError,
        match="must be an object",
    ):
        load_enabled_stocks(config_path)


def test_enabled_stock_missing_symbol(tmp_path):
    config_path = write_config(
        tmp_path,
        {
            "stocks": [
                {
                    "name": "NVIDIA",
                    "enabled": True,
                }
            ]
        },
    )

    with pytest.raises(
        MarketauxConfigError,
        match="invalid symbol",
    ):
        load_enabled_stocks(config_path)


def test_enabled_stock_missing_name(tmp_path):
    config_path = write_config(
        tmp_path,
        {
            "stocks": [
                {
                    "symbol": "NVDA",
                    "enabled": True,
                }
            ]
        },
    )

    with pytest.raises(
        MarketauxConfigError,
        match="invalid name",
    ):
        load_enabled_stocks(config_path)


def test_duplicate_enabled_symbols(tmp_path):
    config_path = write_config(
        tmp_path,
        {
            "stocks": [
                {
                    "symbol": "NVDA",
                    "name": "NVIDIA",
                    "enabled": True,
                },
                {
                    "symbol": "nvda",
                    "name": "NVIDIA Corporation",
                    "enabled": True,
                },
            ]
        },
    )

    with pytest.raises(
        MarketauxConfigError,
        match="Duplicate enabled stock symbol",
    ):
        load_enabled_stocks(config_path)


def test_disabled_invalid_stock_is_ignored(tmp_path):
    config_path = write_config(
        tmp_path,
        {
            "stocks": [
                {
                    "symbol": "",
                    "name": "",
                    "enabled": False,
                }
            ]
        },
    )

    assert load_enabled_stocks(config_path) == []
