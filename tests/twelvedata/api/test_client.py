
import polars as pl
import pytest
import requests

import market_data.api.twelvedata.client as client_module

from market_data.api.twelvedata.client import TwelveDataClient
from market_data.api.twelvedata.exceptions import TwelveDataAPIError


class MockRateLimiter:
    def __init__(self):
        self.calls = 0

    def wait_for_slot(self):
        self.calls += 1


@pytest.fixture
def rate_limiter():
    return MockRateLimiter()


@pytest.fixture
def client(rate_limiter):
    return TwelveDataClient(
        api_key="test_api_key",
        rate_limiter=rate_limiter
    )


@pytest.fixture
def sample_payload():
    return {
        "meta": {
            "symbol": "NVDA",
            "interval": "1day"
        },
        "values": [
            {
                "datetime": "2026-11-04",
                "open": "200.50",
                "high": "205.00",
                "low": "198.00",
                "close": "203.75",
                "volume": "1000000"
            },
            {
                "datetime": "2026-11-05",
                "open": "203.75",
                "high": "210.00",
                "low": "202.00",
                "close": "208.50",
                "volume": "1200000"
            }
        ],
        "status": "ok"
    }


def mock_response(payload, status_code=200):
    response = requests.Response()
    response.status_code = status_code
    response._content = b"{}"

    response.json = lambda: payload

    return response


def test_successful_stock_history(
    client,
    rate_limiter,
    sample_payload,
    monkeypatch
):
    response = mock_response(sample_payload)

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    df = client.get_stock_history(
        symbol="NVDA",
        start_date="2026-11-04",
        end_date="2026-11-05"
    )

    assert isinstance(df, pl.DataFrame)
    assert df.height == 2

    assert df.columns == [
        "asset_name",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    assert df["asset_name"].to_list() == ["NVDA", "NVDA"]
    assert df["close"].to_list() == [203.75, 208.50]
    assert df["volume"].to_list() == [1000000, 1200000]

    assert df.schema["date"] == pl.Datetime("ns")
    assert rate_limiter.calls == 1


def test_empty_values_returns_empty_dataframe(
    client,
    monkeypatch
):
    response = mock_response({
        "status": "ok",
        "values": []
    })

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    df = client.get_stock_history(
        "NVDA",
        "2026-11-04",
        "2026-11-05"
    )

    assert df.is_empty()
    assert df.columns == TwelveDataClient.empty_stock_dataframe().columns


def test_no_data_response_returns_empty_dataframe(
    client,
    monkeypatch
):
    response = mock_response(
        {"code": 400, "message": "No data found"},
        status_code=400
    )

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    df = client.get_stock_history(
        "NVDA",
        "2026-11-04",
        "2026-11-05"
    )

    assert df.is_empty()


def test_api_error_raises_exception(
    client,
    monkeypatch
):
    response = mock_response({
        "status": "error",
        "message": "Invalid API key"
    })

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    with pytest.raises(
        TwelveDataAPIError,
        match="Invalid API key"
    ):
        client.get_stock_history(
            "NVDA",
            "2026-11-04",
            "2026-11-05"
        )


def test_http_error_does_not_expose_api_key(
    client,
    monkeypatch
):
    response = mock_response({}, status_code=500)

    response.raise_for_status = lambda: (
        (_ for _ in ()).throw(
            requests.exceptions.HTTPError(
                "Request failed: apikey=test_api_key"
            )
        )
    )

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    with pytest.raises(TwelveDataAPIError) as exc:
        client.get_stock_history(
            "NVDA",
            "2026-11-04",
            "2026-11-05"
        )

    assert "test_api_key" not in str(exc.value)


def test_network_error_does_not_expose_api_key(
    client,
    monkeypatch
):
    def mock_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError(
            "Connection failed: apikey=test_api_key"
        )

    monkeypatch.setattr(
        client_module.requests,
        "get",
        mock_get
    )

    with pytest.raises(TwelveDataAPIError) as exc:
        client.get_stock_history(
            "NVDA",
            "2026-11-04",
            "2026-11-05"
        )

    assert "test_api_key" not in str(exc.value)


def test_invalid_json_raises_exception(
    client,
    monkeypatch
):
    response = mock_response({})

    response.json = lambda: (_ for _ in ()).throw(
        requests.exceptions.JSONDecodeError(
            "Invalid JSON",
            "invalid",
            0
        )
    )

    monkeypatch.setattr(
        client_module.requests,
        "get",
        lambda *args, **kwargs: response
    )

    with pytest.raises(
        TwelveDataAPIError,
        match="invalid JSON"
    ):
        client.get_stock_history(
            "NVDA",
            "2026-11-04",
            "2026-11-05"
        )


def test_missing_api_key_raises_error(monkeypatch):
    monkeypatch.setattr(
        client_module,
        "load_dotenv",
        lambda *args, **kwargs: None
    )

    monkeypatch.delenv(
        "TWELVE_DATA_API_KEY",
        raising=False
    )

    with pytest.raises(
        ValueError,
        match="TWELVE_DATA_API_KEY not found"
    ):
        TwelveDataClient()


def test_rate_limiter_called_before_request(
    client,
    rate_limiter,
    monkeypatch
):
    response = mock_response({"values": []})

    def mock_get(*args, **kwargs):
        assert rate_limiter.calls == 1
        return response

    monkeypatch.setattr(
        client_module.requests,
        "get",
        mock_get
    )

    client.get_stock_history(
        "NVDA",
        "2026-11-04",
        "2026-11-05"
    )

    assert rate_limiter.calls == 1
