
import importlib
from types import SimpleNamespace

import pytest

from alphavantage_trackers.api.quota_storage import QuotaStorage
from alphavantage_trackers.api.exceptions import (
    AlphaVantageQuotaExceededError
)
from alphavantage_trackers.api.alphavantage_rate_limiter import RateLimiter


rate_limiter_module = importlib.import_module(
    "alphavantage_trackers.api.alphavantage_rate_limiter"
)


@pytest.fixture
def limiter(tmp_path):
    """Create an isolated rate limiter using a temporary SQLite database."""

    storage = QuotaStorage(tmp_path / "test_api_usage.sqlite3")

    return RateLimiter(
        storage=storage,
        max_requests=25,
        window_seconds=86400,
        min_interval=1.5
    )


def insert_requests(storage, timestamps):
    """Insert simulated API request timestamps into SQLite."""

    with storage.connect() as conn:
        conn.executemany(
            """
            INSERT INTO api_requests (requested_at)
            VALUES (?)
            """,
            [(timestamp,) for timestamp in timestamps]
        )
        conn.commit()


def get_request_count(storage):
    """Return the number of currently recorded requests."""

    with storage.connect() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM api_requests"
        ).fetchone()

    return result[0]


def set_fake_time(monkeypatch, initial_time):
    """Replace the limiter's clock and sleep with simulated versions."""

    clock = {"now": initial_time, "sleep_calls": []}

    def fake_sleep(duration):
        clock["sleep_calls"].append(duration)
        clock["now"] += duration

    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=fake_sleep
    )

    monkeypatch.setattr(rate_limiter_module, "time", fake_time)

    return clock


def test_empty_quota_allows_request(limiter, monkeypatch):
    """An empty quota should allow a request immediately."""

    now = 100000
    set_fake_time(monkeypatch, now)

    limiter.acquire()

    assert get_request_count(limiter.storage) == 1


def test_24_requests_allow_25th_request(limiter, monkeypatch):
    """The 25th request should still be permitted."""

    now = 100000
    set_fake_time(monkeypatch, now)

    timestamps = [
        now - 1000 + (i * 10)
        for i in range(24)
    ]

    insert_requests(limiter.storage, timestamps)

    limiter.acquire()

    assert get_request_count(limiter.storage) == 25


def test_25_requests_raise_quota_exception(limiter, monkeypatch):
    """A full quota should raise an exception instead of sleeping."""

    now = 100000
    set_fake_time(monkeypatch, now)

    timestamps = [
        now - 1000 + (i * 10)
        for i in range(25)
    ]

    insert_requests(limiter.storage, timestamps)

    with pytest.raises(AlphaVantageQuotaExceededError) as exc_info:
        limiter.acquire()

    assert exc_info.value.retry_after == (
        timestamps[0] + 86400 - now
    )

    assert get_request_count(limiter.storage) == 25


def test_expired_requests_are_removed(limiter, monkeypatch):
    """Expired requests should be deleted before checking the quota."""

    now = 100000
    set_fake_time(monkeypatch, now)

    expired_timestamp = now - 86401

    active_timestamps = [
        now - 1000 + (i * 10)
        for i in range(24)
    ]

    insert_requests(
        limiter.storage,
        [expired_timestamp] + active_timestamps
    )

    limiter.acquire()

    assert get_request_count(limiter.storage) == 25

    with limiter.storage.connect() as conn:
        expired_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM api_requests
            WHERE requested_at <= ?
            """,
            (now - 86400,)
        ).fetchone()[0]

    assert expired_count == 0


def test_minimum_interval_is_enforced(limiter, monkeypatch):
    """A second request should wait until the minimum interval passes."""

    now = 100000
    clock = set_fake_time(monkeypatch, now)

    limiter.acquire()
    limiter.acquire()

    assert len(clock["sleep_calls"]) == 1
    assert clock["sleep_calls"][0] == pytest.approx(1.5)

    with limiter.storage.connect() as conn:
        timestamps = conn.execute(
            """
            SELECT requested_at
            FROM api_requests
            ORDER BY requested_at ASC
            """
        ).fetchall()

    assert len(timestamps) == 2

    interval = timestamps[1][0] - timestamps[0][0]

    assert interval == pytest.approx(1.5)
