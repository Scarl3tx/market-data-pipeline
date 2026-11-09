
import json

import pytest

from datetime import date, datetime, timezone

from market_data.api.twelvedata.rate_limiter import (
    TwelveDataRateLimiter,
)

from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)


def test_first_request_creates_state_file(tmp_path):
    state_path = tmp_path / "usage.json"

    limiter = TwelveDataRateLimiter(
        state_path=state_path,
    )

    limiter.wait_for_slot()

    assert state_path.exists()

    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["credits_used"] == 1
    assert len(state["request_timestamps"]) == 1


def test_daily_limit_raises_exception(tmp_path):
    limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "usage.json",
        daily_limit=2,
        minimum_interval=0,
    )

    limiter.wait_for_slot()
    limiter.wait_for_slot()

    with pytest.raises(TwelveDataDailyLimitError):
        limiter.wait_for_slot()


def test_minute_limit_waits_for_available_slot(tmp_path):
    current_time = [100.0]
    sleep_calls = []

    def fake_clock():
        return current_time[0]

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        current_time[0] += seconds

    limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "usage.json",
        minute_limit=2,
        minimum_interval=8.1,
        clock=fake_clock,
        sleeper=fake_sleep,
    )

    limiter.wait_for_slot()
    limiter.wait_for_slot()
    limiter.wait_for_slot()

    state = json.loads(
        (tmp_path / "usage.json").read_text(encoding="utf-8")
    )

    assert current_time[0] >= 160.0
    assert len(sleep_calls) == 2
    assert state["credits_used"] == 3
    assert len(state["request_timestamps"]) == 2


def test_daily_usage_resets_on_new_utc_date(tmp_path):
    old_date = date(2026, 11, 6)
    new_date = date(2026, 11, 7)

    old_timestamp = datetime(
        old_date.year,
        old_date.month,
        old_date.day,
        tzinfo=timezone.utc,
    ).timestamp()

    new_timestamp = datetime(
        new_date.year,
        new_date.month,
        new_date.day,
        tzinfo=timezone.utc,
    ).timestamp()

    state_path = tmp_path / "usage.json"

    state_path.write_text(
        json.dumps({
            "date": old_date.isoformat(),
            "credits_used": 790,
            "request_timestamps": [old_timestamp],
        }),
        encoding="utf-8",
    )

    limiter = TwelveDataRateLimiter(
        state_path=state_path,
        clock=lambda: new_timestamp,
    )

    limiter.wait_for_slot()

    state = json.loads(
        state_path.read_text(encoding="utf-8")
    )

    assert state["date"] == new_date.isoformat()
    assert state["credits_used"] == 1
    assert len(state["request_timestamps"]) == 1


def test_corrupted_json_raises_error(tmp_path):
    state_path = tmp_path / "usage.json"
    state_path.write_text(
        '{"date":',
        encoding="utf-8",
    )

    limiter = TwelveDataRateLimiter(
        state_path=state_path,
    )

    with pytest.raises(ValueError, match="invalid JSON"):
        limiter.wait_for_slot()


@pytest.mark.parametrize(
    "state",
    [
        [],
        None,
        "invalid",
        {"date": "2026-11-07"},
        {
            "date": "2026-11-07",
            "credits_used": -1,
            "request_timestamps": [],
        },
        {
            "date": "2026-11-07",
            "credits_used": 1.5,
            "request_timestamps": [],
        },
        {
            "date": "2026-11-07",
            "credits_used": 1,
            "request_timestamps": "invalid",
        },
        {
            "date": "2026-11-07",
            "credits_used": 1,
            "request_timestamps": ["invalid"],
        },
    ],
)
def test_invalid_current_day_state_raises_error(
    tmp_path,
    state,
):
    state_path = tmp_path / "usage.json"
    state_path.write_text(
        json.dumps(state),
        encoding="utf-8",
    )

    limiter = TwelveDataRateLimiter(
        state_path=state_path,
    )

    with pytest.raises(ValueError):
        limiter.wait_for_slot()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"daily_limit": 0},
        {"daily_limit": -1},
        {"minute_limit": 0},
        {"minute_limit": -1},
        {"minimum_interval": -1},
        {"minimum_interval": float("nan")},
        {"minimum_interval": float("inf")},
    ],
)
def test_invalid_limiter_configuration_raises_error(
    tmp_path,
    kwargs,
):
    with pytest.raises(ValueError):
        TwelveDataRateLimiter(
            state_path=tmp_path / "usage.json",
            **kwargs,
        )


def test_save_uses_atomic_replacement(tmp_path):
    state_path = tmp_path / "usage.json"

    limiter = TwelveDataRateLimiter(
        state_path=state_path,
        minimum_interval=0,
    )

    limiter.wait_for_slot()

    assert state_path.exists()
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []

def test_storage_backend_creates_rate_limit_state(tmp_path):
    from market_data.storage.local import LocalStorage

    storage = LocalStorage(tmp_path)

    limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "unused.json",
        minimum_interval=0,
        storage=storage,
    )

    limiter.wait_for_slot()

    assert storage.exists("state/twelvedata/rate_limit.json")

    state = storage.read_json(
        "state/twelvedata/rate_limit.json"
    )

    assert state["credits_used"] == 1
    assert len(state["request_timestamps"]) == 1


def test_storage_backend_preserves_daily_usage_between_instances(
    tmp_path,
):
    from market_data.storage.local import LocalStorage

    storage = LocalStorage(tmp_path)

    first_limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "unused.json",
        daily_limit=2,
        minimum_interval=0,
        storage=storage,
    )

    first_limiter.wait_for_slot()

    second_limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "unused.json",
        daily_limit=2,
        minimum_interval=0,
        storage=storage,
    )

    second_limiter.wait_for_slot()

    with pytest.raises(TwelveDataDailyLimitError):
        second_limiter.wait_for_slot()


def test_storage_backend_resets_usage_on_new_utc_date(tmp_path):
    from market_data.storage.local import LocalStorage

    old_date = date(2026, 11, 6)
    new_date = date(2026, 11, 7)

    old_timestamp = datetime(
        old_date.year,
        old_date.month,
        old_date.day,
        tzinfo=timezone.utc,
    ).timestamp()

    new_timestamp = datetime(
        new_date.year,
        new_date.month,
        new_date.day,
        tzinfo=timezone.utc,
    ).timestamp()

    storage = LocalStorage(tmp_path)

    storage.write_json(
        "state/twelvedata/rate_limit.json",
        {
            "date": old_date.isoformat(),
            "credits_used": 790,
            "request_timestamps": [old_timestamp],
        },
    )

    limiter = TwelveDataRateLimiter(
        state_path=tmp_path / "unused.json",
        clock=lambda: new_timestamp,
        minimum_interval=0,
        storage=storage,
    )

    limiter.wait_for_slot()

    state = storage.read_json(
        "state/twelvedata/rate_limit.json"
    )

    assert state["date"] == new_date.isoformat()
    assert state["credits_used"] == 1
    assert len(state["request_timestamps"]) == 1
