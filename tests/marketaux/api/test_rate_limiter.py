
import json

import pytest

from market_data.api.marketaux.rate_limiter import (
    MarketauxRateLimiter,
)


def test_first_request_does_not_wait(tmp_path):
    state_path = tmp_path / "rate_limit.json"
    calls = []

    limiter = MarketauxRateLimiter(
        state_path=state_path,
        clock=lambda: 100.0,
        sleeper=lambda seconds: calls.append(seconds),
    )

    limiter.wait_for_slot()

    assert calls == []

    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["last_request"] == 100.0


def test_minimum_interval_is_enforced(tmp_path):
    state_path = tmp_path / "rate_limit.json"

    current_time = [100.0]
    sleep_calls = []

    def fake_clock():
        return current_time[0]

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        current_time[0] += seconds

    limiter = MarketauxRateLimiter(
        state_path=state_path,
        minimum_interval=2.0,
        clock=fake_clock,
        sleeper=fake_sleep,
    )

    limiter.wait_for_slot()
    limiter.wait_for_slot()

    assert len(sleep_calls) == 1
    assert abs(sleep_calls[0] - 2.0) < 1e-9


def test_zero_interval_does_not_wait(tmp_path):
    current_time = [100.0]

    limiter = MarketauxRateLimiter(
        state_path=tmp_path / "rate_limit.json",
        minimum_interval=0,
        clock=lambda: current_time[0],
        sleeper=lambda seconds: None,
    )

    limiter.wait_for_slot()
    limiter.wait_for_slot()


@pytest.mark.parametrize(
    "minimum_interval",
    [
        -1,
        float("nan"),
        float("inf"),
    ],
)
def test_invalid_interval_raises_error(
    tmp_path,
    minimum_interval,
):
    with pytest.raises(ValueError):
        MarketauxRateLimiter(
            state_path=tmp_path / "rate_limit.json",
            minimum_interval=minimum_interval,
        )


def test_cooldown_is_persisted_and_enforced(tmp_path):
    state_path = tmp_path / "rate_limit.json"

    current_time = [100.0]
    sleep_calls = []

    def fake_clock():
        return current_time[0]

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        current_time[0] += seconds

    limiter = MarketauxRateLimiter(
        state_path=state_path,
        minimum_interval=3.0,
        clock=fake_clock,
        sleeper=fake_sleep,
    )

    limiter.wait_for_slot()
    limiter.defer_for(10)

    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["last_request"] == 100.0
    assert state["cooldown_until"] == 110.0

    limiter.wait_for_slot()

    assert sleep_calls == [10.0]
    assert current_time[0] == 110.0

    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["last_request"] == 110.0
    assert state["cooldown_until"] == 110.0


def test_new_cooldown_does_not_shorten_existing_cooldown(tmp_path):
    state_path = tmp_path / "rate_limit.json"
    current_time = [100.0]

    limiter = MarketauxRateLimiter(
        state_path=state_path,
        clock=lambda: current_time[0],
        sleeper=lambda seconds: None,
    )

    limiter.defer_for(300)
    limiter.defer_for(60)

    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["cooldown_until"] == 400.0


def test_corrupted_json_raises_error(tmp_path):
    state_path = tmp_path / "rate_limit.json"
    state_path.write_text(
        '{"last_request":',
        encoding="utf-8",
    )

    limiter = MarketauxRateLimiter(
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
        {"last_request": "invalid"},
        {"last_request": True},
        {"cooldown_until": float("nan")},
        {"cooldown_until": float("inf")},
    ],
)
def test_invalid_state_raises_error(tmp_path, state):
    state_path = tmp_path / "rate_limit.json"
    state_path.write_text(
        json.dumps(state),
        encoding="utf-8",
    )

    limiter = MarketauxRateLimiter(
        state_path=state_path,
    )

    with pytest.raises(ValueError):
        limiter.wait_for_slot()


@pytest.mark.parametrize(
    "seconds",
    [
        -1,
        float("nan"),
        float("inf"),
        True,
        "invalid",
    ],
)
def test_invalid_cooldown_raises_error(tmp_path, seconds):
    limiter = MarketauxRateLimiter(
        state_path=tmp_path / "rate_limit.json",
    )

    with pytest.raises(ValueError):
        limiter.defer_for(seconds)


def test_save_uses_atomic_replacement(tmp_path):
    state_path = tmp_path / "rate_limit.json"

    limiter = MarketauxRateLimiter(
        state_path=state_path,
    )

    limiter.wait_for_slot()

    assert state_path.exists()
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
