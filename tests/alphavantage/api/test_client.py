import pytest
import requests

from unittest.mock import Mock, patch

from market_data.api.alphavantage.client import AlphaVantageClient
from market_data.api.alphavantage.exceptions import (
    AlphaVantageAPIError,
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError,
)


@pytest.fixture
def client():
    return AlphaVantageClient(
        api_key="test_key",
        rate_limiter=Mock()
    )


@patch("market_data.api.alphavantage.client.requests.get")
def test_get_news_sentiment_success(mock_get, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "items": "1",
        "feed": [{"title": "Test article"}],
    }
    mock_get.return_value = mock_response

    result = client.get_news_sentiment(tickers=["NVDA"])

    assert result["feed"][0]["title"] == "Test article"

    params = mock_get.call_args.kwargs["params"]
    assert params["function"] == "NEWS_SENTIMENT"
    assert params["tickers"] == "NVDA"
    assert params["apikey"] == "test_key"


@patch("market_data.api.alphavantage.client.requests.get")
def test_handles_http_error(mock_get, client):
    mock_get.side_effect = requests.exceptions.HTTPError("HTTP error")

    with pytest.raises(AlphaVantageAPIError):
        client.get_news_sentiment()


@patch("market_data.api.alphavantage.client.requests.get")
def test_handles_invalid_json(mock_get, client):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_response

    with pytest.raises(AlphaVantageAPIError):
        client.get_news_sentiment()


@pytest.mark.parametrize(
    "response_data, expected_exception",
    [
        (
            {"Note": "API call frequency exceeded"},
            AlphaVantageRateLimitError,
        ),
        (
            {"Information": "API daily limit reached"},
            AlphaVantageQuotaExceededError,
        ),
        (
            {"Error Message": "Invalid API call"},
            AlphaVantageAPIError,
        ),
        (
            {"unexpected": "response"},
            AlphaVantageAPIError,
        ),
    ],
)
@patch("market_data.api.alphavantage.client.requests.get")
def test_handles_api_errors(
    mock_get,
    client,
    response_data,
    expected_exception,
):
    mock_response = Mock()
    mock_response.json.return_value = response_data
    mock_get.return_value = mock_response

    with pytest.raises(expected_exception):
        client.get_news_sentiment()

def test_rate_limiter_is_called(client):
    with patch(
        "market_data.api.alphavantage.client.requests.get"
    ) as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = {
            "feed": []
        }
        mock_get.return_value = mock_response

        client.get_news_sentiment()

    client.rate_limiter.acquire.assert_called_once()


def test_rate_limiter_failure_prevents_api_request(client):
    client.rate_limiter.acquire.side_effect = (
        AlphaVantageQuotaExceededError(retry_after=3600)
    )

    with patch(
        "market_data.api.alphavantage.client.requests.get"
    ) as mock_get:
        with pytest.raises(AlphaVantageQuotaExceededError):
            client.get_news_sentiment()

    mock_get.assert_not_called()

from unittest.mock import Mock


def test_news_sentiment_uses_maximum_limit_by_default(
    client, monkeypatch
):
    response = Mock()
    response.json.return_value = {"feed": []}
    response.raise_for_status.return_value = None

    mock_get = Mock(return_value=response)

    monkeypatch.setattr(
        "market_data.api.alphavantage.client.requests.get",
        mock_get,
    )

    client.get_news_sentiment()

    params = mock_get.call_args.kwargs["params"]

    assert params["limit"] == 1000
    assert params["sort"] == "LATEST"


def test_news_sentiment_accepts_time_range_and_sort(
    client, monkeypatch
):
    response = Mock()
    response.json.return_value = {"feed": []}
    response.raise_for_status.return_value = None

    mock_get = Mock(return_value=response)

    monkeypatch.setattr(
        "market_data.api.alphavantage.client.requests.get",
        mock_get,
    )

    client.get_news_sentiment(
        tickers=["NVDA"],
        time_from="20260101T0000",
        time_to="20261106T2359",
        sort="EARLIEST",
    )

    params = mock_get.call_args.kwargs["params"]

    assert params["tickers"] == "NVDA"
    assert params["time_from"] == "20260101T0000"
    assert params["time_to"] == "20261106T2359"
    assert params["sort"] == "EARLIEST"
    assert params["limit"] == 1000


def test_news_sentiment_rejects_invalid_limit(client):
    import pytest

    with pytest.raises(ValueError):
        client.get_news_sentiment(limit=1001)


def test_news_sentiment_rejects_invalid_sort(client):
    import pytest

    with pytest.raises(ValueError):
        client.get_news_sentiment(sort="INVALID")