
from unittest.mock import Mock

import pytest
import requests

from market_data.api.marketaux.client import (
    MarketauxClient,
    BASE_URL,
)
from market_data.api.marketaux.exceptions import (
    MarketauxAPIError,
    MarketauxAuthenticationError,
    MarketauxQuotaExceededError,
    MarketauxEndpointRestrictedError,
    MarketauxRateLimitError,
    MarketauxResponseError,
)


class FakeQuotaTracker:
    def __init__(self):
        self.calls = 0
        self.fail = False

    def reserve_request(self):
        if self.fail:
            raise RuntimeError("Daily limit exhausted.")

        self.calls += 1


class FakeRateLimiter:
    def __init__(self):
        self.wait_calls = 0
        self.cooldowns = []

    def wait_for_slot(self):
        self.wait_calls += 1

    def defer_for(self, seconds):
        self.cooldowns.append(seconds)


def make_client(rate_limiter=None, timeout=30):
    return MarketauxClient(
        api_token="test-token",
        timeout=timeout,
        quota_tracker=FakeQuotaTracker(),
        rate_limiter=rate_limiter or FakeRateLimiter(),
        daily_limit=100,
    )


def make_response(status_code=200, payload=None, headers=None):
    response = Mock()
    response.status_code = status_code
    response.headers = headers or {}

    if payload is None:
        payload = {
            "meta": {
                "found": 1,
                "returned": 1,
                "limit": 3,
                "page": 1,
            },
            "data": [
                {
                    "uuid": "test-uuid",
                    "title": "Test article",
                    "entities": [
                        {
                            "symbol": "NVDA",
                            "sentiment_score": 0.75,
                        }
                    ],
                }
            ],
        }

    response.json.return_value = payload

    if status_code >= 400:
        response.raise_for_status.side_effect = (
            requests.HTTPError("HTTP error")
        )

    return response


def test_get_news_success(monkeypatch):
    client = make_client()
    response = make_response()

    request = Mock(return_value=response)

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        request,
    )

    result = client.get_news(
        symbols=["NVDA", "AMD"],
        page=1,
    )

    assert result["meta"]["returned"] == 1

    args, kwargs = request.call_args

    assert args[0] == BASE_URL
    assert kwargs["params"]["symbols"] == "NVDA,AMD"
    assert kwargs["params"]["api_token"] == "test-token"
    assert kwargs["params"]["limit"] == 3
    assert kwargs["params"]["page"] == 1
    assert kwargs["params"]["filter_entities"] == "true"
    assert kwargs["timeout"] == 30


def test_get_news_passes_date_filters(monkeypatch):
    client = make_client()

    request = Mock(return_value=make_response())

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        request,
    )

    client.get_news(
        symbols=["NVDA"],
        published_after="2026-11-01",
        published_before="2026-11-02",
    )

    params = request.call_args.kwargs["params"]

    assert params["published_after"] == "2026-11-01"
    assert params["published_before"] == "2026-11-02"


def test_get_news_rejects_empty_symbols():
    client = make_client()

    with pytest.raises(ValueError):
        client.get_news(symbols=[])


def test_get_news_rejects_excessive_limit():
    client = make_client()

    with pytest.raises(ValueError):
        client.get_news(
            symbols=["NVDA"],
            limit=4,
        )


@pytest.mark.parametrize(
    "invalid_page",
    [0, -1, 1.5, "1", True, None],
)
def test_get_news_rejects_invalid_page(invalid_page):
    client = make_client()

    with pytest.raises(ValueError):
        client.get_news(
            ["NVDA"],
            page=invalid_page,
        )


@pytest.mark.parametrize(
    "invalid_limit",
    [0, -1, 1.5, "2", True],
)
def test_get_news_rejects_invalid_limit(invalid_limit):
    client = make_client()

    with pytest.raises(ValueError):
        client.get_news(
            ["NVDA"],
            limit=invalid_limit,
        )


@pytest.mark.parametrize(
    "invalid_timeout",
    [0, -1, float("inf"), float("nan"), True, "30", None],
)
def test_client_rejects_invalid_timeout(invalid_timeout):
    with pytest.raises(ValueError):
        make_client(timeout=invalid_timeout)


def test_client_accepts_positive_timeout():
    client = make_client(timeout=12.5)

    assert client.timeout == 12.5


def test_get_news_handles_authentication_error(monkeypatch):
    client = make_client()

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=make_response(401)),
    )

    with pytest.raises(MarketauxAuthenticationError):
        client.get_news(["NVDA"])


def test_get_news_handles_quota_error(monkeypatch):
    client = make_client()

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=make_response(402)),
    )

    with pytest.raises(MarketauxQuotaExceededError):
        client.get_news(["NVDA"])


def test_get_news_handles_restricted_endpoint(monkeypatch):
    client = make_client()

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=make_response(403)),
    )

    with pytest.raises(MarketauxEndpointRestrictedError):
        client.get_news(["NVDA"])


def test_get_news_handles_rate_limit(monkeypatch):
    rate_limiter = FakeRateLimiter()
    client = make_client(rate_limiter=rate_limiter)

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=make_response(429)),
    )

    with pytest.raises(MarketauxRateLimitError):
        client.get_news(["NVDA"])

    assert rate_limiter.cooldowns == [300.0]


def test_get_news_uses_retry_after_header(monkeypatch):
    rate_limiter = FakeRateLimiter()
    client = make_client(rate_limiter=rate_limiter)

    response = make_response(
        429,
        headers={"Retry-After": "60"},
    )

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=response),
    )

    with pytest.raises(MarketauxRateLimitError):
        client.get_news(["NVDA"])

    assert rate_limiter.cooldowns == [60.0]


@pytest.mark.parametrize(
    "retry_after",
    ["inf", "-inf", "nan", "invalid"],
)
def test_get_news_uses_fallback_for_invalid_retry_after(
    monkeypatch,
    retry_after,
):
    rate_limiter = FakeRateLimiter()
    client = make_client(rate_limiter=rate_limiter)

    response = make_response(
        429,
        headers={"Retry-After": retry_after},
    )

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=response),
    )

    with pytest.raises(MarketauxRateLimitError):
        client.get_news(["NVDA"])

    assert rate_limiter.cooldowns == [300.0]


def test_get_news_rejects_invalid_response(monkeypatch):
    client = make_client()

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=make_response(
            payload={"unexpected": "response"}
        )),
    )

    with pytest.raises(MarketauxResponseError):
        client.get_news(["NVDA"])


def test_get_news_handles_invalid_json(monkeypatch):
    client = make_client()

    response = make_response()
    response.json.side_effect = ValueError("Invalid JSON")

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        Mock(return_value=response),
    )

    with pytest.raises(MarketauxResponseError):
        client.get_news(["NVDA"])


@pytest.mark.parametrize(
    "request_error",
    [
        requests.Timeout("timed out"),
        requests.ConnectionError("connection failed"),
        requests.RequestException("request failed"),
    ],
)
def test_get_news_handles_network_errors(
    monkeypatch,
    request_error,
):
    client = make_client()

    request = Mock(side_effect=request_error)

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        request,
    )

    with pytest.raises(MarketauxAPIError):
        client.get_news(["NVDA"])


def test_get_news_does_not_call_api_when_quota_exhausted(
    monkeypatch,
):
    client = make_client()
    client.quota_tracker.fail = True

    request = Mock()

    monkeypatch.setattr(
        "market_data.api.marketaux.client.requests.get",
        request,
    )

    with pytest.raises(MarketauxQuotaExceededError):
        client.get_news(["NVDA"])

    request.assert_not_called()
