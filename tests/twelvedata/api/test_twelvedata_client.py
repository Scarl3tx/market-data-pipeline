
from datetime import datetime
from unittest.mock import Mock

import polars as pl
import pytest
import requests

from market_data.api.twelvedata.client import TwelveDataClient
from market_data.api.twelvedata.exceptions import TwelveDataAPIError


class FakeRateLimiter:

    def __init__(self):
        self.calls = 0

    def wait_for_slot(self):
        self.calls += 1


@pytest.fixture
def rate_limiter():
    return FakeRateLimiter()


@pytest.fixture
def client(rate_limiter):
    return TwelveDataClient(
        api_key="test_api_key",
        rate_limiter=rate_limiter,
    )


def mock_response(
    payload,
    status_code=200,
    raise_error=None,
):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = payload

    if raise_error:
        response.raise_for_status.side_effect = raise_error
    else:
        response.raise_for_status.return_value = None

    return response


def sample_payload():
    return {
        "meta": {
            "symbol": "NVDA",
            "interval": "1day",
        },
        "values": [
            {
                "datetime": "2026-11-07",
                "open": "180.5",
                "high": "185.0",
                "low": "179.0",
                "close": "183.5",
                "volume": "1000000",
            },
            {
                "datetime": "2026-11-06",
                "open": "175.0",
                "high": "181.0",
                "low": "174.0",
                "close": "180.0",
                "volume": "900000",
            },
        ],
    }


def test_successful_stock_history(
    client,
    rate_limiter,
    monkeypatch,
):
    response = mock_response(sample_payload())
    get_mock = Mock(return_value=response)

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        get_mock,
    )

    df = client.get_stock_history(
        symbol="NVDA",
        start_date="2026-11-06",
        end_date="2026-11-07",
    )

    assert df.height == 2
    assert df.columns == [
        "asset_name",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    assert df["asset_name"].to_list() == ["NVDA", "NVDA"]
    assert df["date"].to_list() == [
        datetime(2026, 11, 6),
        datetime(2026, 11, 7),
    ]

    assert df["close"].to_list() == [180.0, 183.5]
    assert df["volume"].to_list() == [900000, 1000000]
    assert rate_limiter.calls == 1

    kwargs = get_mock.call_args.kwargs
    assert kwargs["timeout"] == 30
    assert kwargs["params"]["symbol"] == "NVDA"
    assert kwargs["params"]["interval"] == "1day"
    assert kwargs["params"]["start_date"] == "2026-11-06"
    assert kwargs["params"]["end_date"] == "2026-11-07"
    assert "outputsize" not in kwargs["params"]


def test_symbol_is_normalized(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": [{
                "datetime": "2026-11-07",
                "open": "1",
                "high": "2",
                "low": "1",
                "close": "2",
                "volume": "10",
            }],
        })),
    )

    df = client.get_stock_history(
        " nvda ",
        None,
        "2026-11-07",
    )

    assert df["asset_name"].to_list() == ["NVDA"]


def test_no_data_http_400_returns_empty_dataframe(
    client,
    monkeypatch,
):
    response = mock_response(
        {"message": "No data found for the specified symbol."},
        status_code=400,
        raise_error=requests.exceptions.HTTPError(),
    )

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=response),
    )

    df = client.get_stock_history(
        "NVDA",
        None,
        "2026-11-07",
    )

    assert df.is_empty()
    assert df.schema == client.empty_stock_dataframe().schema


def test_empty_values_returns_empty_dataframe(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({"values": []})),
    )

    df = client.get_stock_history(
        "NVDA",
        None,
        "2026-11-07",
    )

    assert df.is_empty()


def test_api_error_status_raises(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "status": "error",
            "message": "Invalid API key",
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="Invalid API key"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_http_error_raises(
    client,
    monkeypatch,
):
    response = mock_response(
        {"message": "Server error"},
        status_code=500,
        raise_error=requests.exceptions.HTTPError(),
    )

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=response),
    )

    with pytest.raises(TwelveDataAPIError, match="HTTP error"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_invalid_json_raises(
    client,
    monkeypatch,
):
    response = Mock()
    response.status_code = 200
    response.json.side_effect = ValueError("Invalid JSON")

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=response),
    )

    with pytest.raises(TwelveDataAPIError, match="invalid JSON"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_network_error_raises(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(
            side_effect=requests.exceptions.ConnectionError()
        ),
    )

    with pytest.raises(TwelveDataAPIError, match="Request failed"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_missing_values_field_raises(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "meta": {"symbol": "NVDA"},
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="missing the 'values' field"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


@pytest.mark.parametrize(
    "values",
    [
        "invalid",
        {},
        None,
        123,
    ],
)
def test_invalid_values_type_raises(
    client,
    monkeypatch,
    values,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": values,
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="Invalid values format"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_non_dictionary_row_raises(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": ["invalid"],
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="Invalid row"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_missing_column_raises(
    client,
    monkeypatch,
):
    row = sample_payload()["values"][0]
    del row["volume"]

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": [row],
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="Missing columns"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


@pytest.mark.parametrize(
    "value",
    [
        "not-a-date",
        None,
        20261107,
    ],
)
def test_invalid_date_raises(
    client,
    monkeypatch,
    value,
):
    row = sample_payload()["values"][0]
    row["datetime"] = value

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": [row],
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="Invalid date"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


@pytest.mark.parametrize(
    "column",
    ["open", "high", "low", "close"],
)
@pytest.mark.parametrize(
    "value",
    ["invalid", None, "nan", "inf"],
)
def test_invalid_price_value_raises(
    client,
    monkeypatch,
    column,
    value,
):
    row = sample_payload()["values"][0]
    row[column] = value

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": [row],
        })),
    )

    with pytest.raises(TwelveDataAPIError):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


@pytest.mark.parametrize(
    "value",
    [
        "invalid",
        None,
        "nan",
        "inf",
        "1.5",
        "-1",
    ],
)
def test_invalid_volume_raises(
    client,
    monkeypatch,
    value,
):
    row = sample_payload()["values"][0]
    row["volume"] = value

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response({
            "values": [row],
        })),
    )

    with pytest.raises(TwelveDataAPIError, match="volume"):
        client.get_stock_history(
            "NVDA",
            None,
            "2026-11-07",
        )


def test_start_date_filters_old_rows(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        Mock(return_value=mock_response(sample_payload())),
    )

    df = client.get_stock_history(
        "NVDA",
        start_date="2026-11-07",
        end_date="2026-11-07",
    )

    assert df.height == 1
    assert df["date"].to_list() == [datetime(2026, 11, 7)]


@pytest.mark.parametrize(
    "symbol",
    ["", "   ", None, 123],
)
def test_invalid_symbol_rejected_before_request(
    client,
    rate_limiter,
    symbol,
):
    with pytest.raises(ValueError, match="symbol"):
        client.get_stock_history(
            symbol,
            None,
            "2026-11-07",
        )

    assert rate_limiter.calls == 0


@pytest.mark.parametrize(
    "start_date,end_date",
    [
        ("2026-11-08", "2026-11-07"),
        ("2026-02-30", "2026-11-07"),
        ("2026/11/01", "2026-11-07"),
        (None, "2026-02-30"),
    ],
)
def test_invalid_request_dates_rejected_before_request(
    client,
    rate_limiter,
    start_date,
    end_date,
):
    with pytest.raises(ValueError, match="ISO dates"):
        client.get_stock_history(
            "NVDA",
            start_date,
            end_date,
        )

    assert rate_limiter.calls == 0

def test_request_without_start_date_uses_outputsize(
    client,
    monkeypatch,
):
    get_mock = Mock(
        return_value=mock_response({"values": []})
    )

    monkeypatch.setattr(
        "market_data.api.twelvedata.client.requests.get",
        get_mock,
    )

    client.get_stock_history(
        "NVDA",
        None,
        "2026-11-07",
    )

    params = get_mock.call_args.kwargs["params"]

    assert params["end_date"] == "2026-11-07"
    assert params["outputsize"] == 5000
    assert "start_date" not in params