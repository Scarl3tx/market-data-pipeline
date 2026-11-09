
import json

import pytest

from market_data.config import load_stocks_config


def write_config(tmp_path, monkeypatch, config):
    config_path = tmp_path / "stocks_config.json"
    config_path.write_text(
        json.dumps(config),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "market_data.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    return config_path


def valid_stock(**overrides):
    stock = {
        "symbol": "NVDA",
        "name": "NVIDIA",
        "enabled": True,
        "start_date": "2000-01-01",
    }
    stock.update(overrides)
    return stock


def test_load_stocks_config(tmp_path, monkeypatch):
    config = {
        "stocks": [
            valid_stock(),
            valid_stock(
                symbol="AMD",
                name="AMD",
                enabled=False,
            ),
        ]
    }

    write_config(tmp_path, monkeypatch, config)

    stocks = load_stocks_config()

    assert len(stocks) == 1
    assert stocks[0]["symbol"] == "NVDA"
    assert stocks[0]["name"] == "NVIDIA"


def test_invalid_stocks_config_raises_error(tmp_path, monkeypatch):
    write_config(tmp_path, monkeypatch, {"stocks": "NVDA"})

    with pytest.raises(ValueError, match="'stocks' list"):
        load_stocks_config()


def test_invalid_json_raises_error(tmp_path, monkeypatch):
    config_path = tmp_path / "stocks_config.json"
    config_path.write_text('{"stocks": [', encoding="utf-8")

    monkeypatch.setattr(
        "market_data.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    with pytest.raises(ValueError, match="Invalid JSON"):
        load_stocks_config()


@pytest.mark.parametrize("config", [[], "NVDA", None, 123])
def test_non_object_root_raises_error(tmp_path, monkeypatch, config):
    write_config(tmp_path, monkeypatch, config)

    with pytest.raises(ValueError, match="JSON object"):
        load_stocks_config()


@pytest.mark.parametrize("stocks", [None, "NVDA", {}, 123])
def test_invalid_stocks_value_raises_error(tmp_path, monkeypatch, stocks):
    write_config(tmp_path, monkeypatch, {"stocks": stocks})

    with pytest.raises(ValueError, match="'stocks' list"):
        load_stocks_config()


@pytest.mark.parametrize("stock", [None, "NVDA", [], 123])
def test_non_object_stock_entry_raises_error(tmp_path, monkeypatch, stock):
    write_config(tmp_path, monkeypatch, {"stocks": [stock]})

    with pytest.raises(ValueError, match=r"stocks\[0\].*JSON object"):
        load_stocks_config()


@pytest.mark.parametrize(
    "overrides, expected_message",
    [
        ({"symbol": None}, "'symbol'"),
        ({"symbol": ""}, "'symbol'"),
        ({"symbol": "   "}, "'symbol'"),
        ({"symbol": "NV DA"}, "'symbol'"),
        ({"name": None}, "'name'"),
        ({"name": ""}, "'name'"),
        ({"enabled": "true"}, "'enabled'"),
        ({"enabled": 1}, "'enabled'"),
        ({"enabled": "false"}, "'enabled'"),
        ({"enabled": 0}, "'enabled'"),
        ({"enabled": None}, "'enabled'"),
        ({"enabled": []}, "'enabled'"),
        ({"enabled": {}}, "'enabled'"),
        ({"start_date": None}, "'start_date'"),
        ({"start_date": "01-01-2000"}, "'start_date'"),
        ({"start_date": "2000-02-30"}, "'start_date'"),
        ({"start_date": "2000-1-01"}, "'start_date'"),
    ],
)
def test_invalid_stock_fields_raise_error(
    tmp_path,
    monkeypatch,
    overrides,
    expected_message,
):
    write_config(
        tmp_path,
        monkeypatch,
        {"stocks": [valid_stock(**overrides)]},
    )

    with pytest.raises(ValueError, match=expected_message):
        load_stocks_config()


def test_disabled_stock_is_still_validated(tmp_path, monkeypatch):
    invalid_stock = valid_stock(symbol="", enabled=False)

    write_config(
        tmp_path,
        monkeypatch,
        {"stocks": [invalid_stock]},
    )

    with pytest.raises(ValueError, match="'symbol'"):
        load_stocks_config()


def test_duplicate_symbols_raise_error(tmp_path, monkeypatch):
    config = {
        "stocks": [
            valid_stock(),
            valid_stock(
                symbol="nvda",
                name="Another NVIDIA entry",
            ),
        ]
    }

    write_config(tmp_path, monkeypatch, config)

    with pytest.raises(ValueError, match="Duplicate stock symbol 'NVDA'"):
        load_stocks_config()


def test_symbols_and_names_are_normalized(tmp_path, monkeypatch):
    config = {
        "stocks": [
            valid_stock(
                symbol=" nvda ",
                name=" NVIDIA ",
            )
        ]
    }

    write_config(tmp_path, monkeypatch, config)

    stocks = load_stocks_config()

    assert stocks[0]["symbol"] == "NVDA"
    assert stocks[0]["name"] == "NVIDIA"


def test_extra_configuration_fields_are_preserved(tmp_path, monkeypatch):
    config = {
        "stocks": [
            valid_stock(
                exchange="NASDAQ",
                currency="USD",
            )
        ]
    }

    write_config(tmp_path, monkeypatch, config)

    stocks = load_stocks_config()

    assert stocks[0]["exchange"] == "NASDAQ"
    assert stocks[0]["currency"] == "USD"


def test_empty_stocks_list_returns_empty_list(tmp_path, monkeypatch):
    write_config(tmp_path, monkeypatch, {"stocks": []})

    assert load_stocks_config() == []


def test_missing_config_file_raises_error(tmp_path, monkeypatch):
    config_path = tmp_path / "missing_stocks_config.json"

    monkeypatch.setattr(
        "market_data.config.STOCKS_CONFIG_PATH",
        config_path,
    )

    with pytest.raises(FileNotFoundError):
        load_stocks_config()
