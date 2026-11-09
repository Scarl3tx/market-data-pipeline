
from concurrent.futures import ThreadPoolExecutor

import pytest

from market_data.api.marketaux.quota_tracker import (
    MarketauxQuotaTracker,
)


def test_initial_usage_is_zero(tmp_path):
    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=100,
    )

    assert tracker.get_usage() == {
        "used": 0,
        "limit": 100,
        "remaining": 100,
    }


def test_reserve_request_increments_usage(tmp_path):
    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=100,
    )

    assert tracker.reserve_request() == 1
    assert tracker.reserve_request() == 2

    assert tracker.get_usage() == {
        "used": 2,
        "limit": 100,
        "remaining": 98,
    }


def test_daily_limit_is_enforced(tmp_path):
    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=2,
    )

    tracker.reserve_request()
    tracker.reserve_request()

    with pytest.raises(RuntimeError):
        tracker.reserve_request()

    assert tracker.get_usage()["used"] == 2


def test_usage_resets_on_new_utc_date(tmp_path, monkeypatch):
    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=2,
    )

    monkeypatch.setattr(
        tracker,
        "_today",
        lambda: "2026-11-06",
    )

    tracker.reserve_request()
    tracker.reserve_request()

    monkeypatch.setattr(
        tracker,
        "_today",
        lambda: "2026-11-07",
    )

    assert tracker.get_usage() == {
        "used": 0,
        "limit": 2,
        "remaining": 2,
    }


def test_usage_persists_after_reinitialization(tmp_path):
    db_path = tmp_path / "usage.sqlite3"

    tracker = MarketauxQuotaTracker(db_path, daily_limit=100)

    tracker.reserve_request()
    tracker.reserve_request()

    new_tracker = MarketauxQuotaTracker(db_path, daily_limit=100)

    assert new_tracker.get_usage()["used"] == 2


def test_concurrent_reservations_never_exceed_daily_limit(tmp_path):
    daily_limit = 7
    total_requests = 20

    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=daily_limit,
    )

    def attempt_reservation():
        try:
            tracker.reserve_request()
            return True
        except RuntimeError:
            return False

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(
            executor.map(
                lambda _: attempt_reservation(),
                range(total_requests),
            )
        )

    assert sum(results) == daily_limit
    assert results.count(False) == total_requests - daily_limit

    assert tracker.get_usage() == {
        "used": daily_limit,
        "limit": daily_limit,
        "remaining": 0,
    }


@pytest.mark.parametrize(
    "invalid_limit",
    [0, -1, 1.5, "100", True, None],
)
def test_invalid_daily_limit_is_rejected(tmp_path, invalid_limit):
    with pytest.raises(ValueError):
        MarketauxQuotaTracker(
            tmp_path / "usage.sqlite3",
            daily_limit=invalid_limit,
        )


def test_database_path_accepts_string(tmp_path):
    db_path = tmp_path / "usage.sqlite3"

    tracker = MarketauxQuotaTracker(db_path)

    assert tracker.db_path == db_path
    assert db_path.exists()


def test_quota_reservation_rolls_back_when_limit_exhausted(tmp_path):
    tracker = MarketauxQuotaTracker(
        tmp_path / "usage.sqlite3",
        daily_limit=1,
    )

    assert tracker.reserve_request() == 1

    with pytest.raises(RuntimeError):
        tracker.reserve_request()

    assert tracker.get_usage()["used"] == 1
