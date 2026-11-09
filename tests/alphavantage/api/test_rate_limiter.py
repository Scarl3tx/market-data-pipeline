import pytest

from unittest.mock import patch

from market_data.api.alphavantage.quota_storage import QuotaStorage
from market_data.api.alphavantage.rate_limiter import AlphaVantageRateLimiter
from market_data.api.alphavantage.exceptions import (
    AlphaVantageQuotaExceededError,
)


@pytest.fixture
def limiter(tmp_path):
    storage = QuotaStorage(tmp_path / "usage.sqlite3")

    return AlphaVantageRateLimiter(
        storage=storage,
        max_requests=25,
        window_seconds=86400,
        min_interval=1.5,
    )


def test_allows_first_request(limiter):
    limiter.acquire()

    with limiter.storage.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM api_requests"
        ).fetchone()[0]

    assert count == 1


def test_allows_requests_until_quota_reached(limiter):
    with limiter.storage.connect() as connection:
        connection.executemany(
            "INSERT INTO api_requests (requested_at) VALUES (?)",
            [(1000 + i * 2,) for i in range(24)],
        )

    with patch("market_data.api.alphavantage.rate_limiter.time.time", return_value=1050):
        limiter.acquire()

    with limiter.storage.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM api_requests"
        ).fetchone()[0]

    assert count == 25


def test_raises_when_daily_quota_exceeded(limiter):
    with limiter.storage.connect() as connection:
        connection.executemany(
            "INSERT INTO api_requests (requested_at) VALUES (?)",
            [(1000 + i * 2,) for i in range(25)],
        )

    with patch("market_data.api.alphavantage.rate_limiter.time.time", return_value=1050):
        with pytest.raises(AlphaVantageQuotaExceededError):
            limiter.acquire()


def test_removes_expired_requests(limiter):
    with limiter.storage.connect() as connection:
        connection.executemany(
            "INSERT INTO api_requests (requested_at) VALUES (?)",
            [(1000 + i,) for i in range(25)],
        )

    with patch("market_data.api.alphavantage.rate_limiter.time.time", return_value=100000):
        limiter.acquire()

    with limiter.storage.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM api_requests"
        ).fetchone()[0]

    assert count == 1


def test_waits_for_minimum_interval(limiter):
    with limiter.storage.connect() as connection:
        connection.execute(
            "INSERT INTO api_requests (requested_at) VALUES (?)",
            (1000,),
        )

    with (
        patch("market_data.api.alphavantage.rate_limiter.time.time", side_effect=[1000.5, 1002]),
        patch("market_data.api.alphavantage.rate_limiter.time.sleep") as mock_sleep,
    ):
        limiter.acquire()

    mock_sleep.assert_called_once_with(1.0)