
import json
from datetime import datetime

import pytest

from market_data.pipelines.twelvedata.bronze.hourly_tracker import (
    _load_state,
    _save_state,
    has_pulled_this_hour,
    record_hourly_pull,
)


def test_missing_state_returns_false(tmp_path):
    state_path = tmp_path / "hourly.json"

    assert has_pulled_this_hour(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 19, 30),
    ) is False


def test_records_hourly_pull(tmp_path):
    state_path = tmp_path / "hourly.json"
    now = datetime(2026, 11, 7, 19, 30)

    record_hourly_pull(
        state_path,
        "NVDA",
        now,
    )

    assert has_pulled_this_hour(
        state_path,
        "NVDA",
        now,
    ) is True


def test_same_hour_is_skipped(tmp_path):
    state_path = tmp_path / "hourly.json"

    record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 19, 5),
    )

    assert has_pulled_this_hour(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 19, 55),
    ) is True


def test_different_hour_is_allowed(tmp_path):
    state_path = tmp_path / "hourly.json"

    record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 19, 55),
    )

    assert has_pulled_this_hour(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 20, 5),
    ) is False


def test_different_date_is_allowed(tmp_path):
    state_path = tmp_path / "hourly.json"

    record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 23, 55),
    )

    assert has_pulled_this_hour(
        state_path,
        "NVDA",
        datetime(2026, 11, 8, 0, 5),
    ) is False


def test_multiple_symbols_are_preserved(tmp_path):
    state_path = tmp_path / "hourly.json"
    now = datetime(2026, 11, 7, 19, 30)

    record_hourly_pull(state_path, "NVDA", now)
    record_hourly_pull(state_path, "AMD", now)

    state = _load_state(state_path)

    assert set(state) == {"NVDA", "AMD"}


def test_recording_same_symbol_updates_timestamp(tmp_path):
    state_path = tmp_path / "hourly.json"

    record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 19, 30),
    )

    record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 7, 20, 30),
    )

    state = _load_state(state_path)

    assert state["NVDA"] == "2026-11-07T20:30:00"


def test_invalid_json_raises_value_error(tmp_path):
    state_path = tmp_path / "hourly.json"

    state_path.write_text(
        "{invalid",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid TwelveData hourly state"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        "invalid",
        123,
        None,
    ],
)
def test_invalid_root_raises_value_error(tmp_path, payload):
    state_path = tmp_path / "hourly.json"

    state_path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="root must be an object"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "timestamp",
    [
        None,
        123,
        [],
        "not-a-date",
    ],
)
def test_invalid_timestamp_raises_value_error(
    tmp_path,
    timestamp,
):
    state_path = tmp_path / "hourly.json"

    state_path.write_text(
        json.dumps({
            "NVDA": timestamp,
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid hourly timestamp"):
        _load_state(state_path)


def test_invalid_symbol_key_raises_value_error(tmp_path):
    state_path = tmp_path / "hourly.json"

    state_path.write_text(
        json.dumps({
            "": "2026-11-07T19:30:00",
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="symbol keys"):
        _load_state(state_path)


def test_save_state_creates_parent_directory(tmp_path):
    state_path = tmp_path / "nested" / "hourly.json"

    _save_state(
        state_path,
        {},
    )

    assert state_path.exists()


def test_save_state_removes_temporary_files(tmp_path):
    state_path = tmp_path / "hourly.json"

    _save_state(
        state_path,
        {"NVDA": "2026-11-07T19:30:00"},
    )

    assert list(tmp_path.glob("hourly.*.tmp")) == []


@pytest.mark.parametrize(
    "symbol",
    [
        "",
        "   ",
        None,
        123,
    ],
)
def test_record_rejects_invalid_symbol(tmp_path, symbol):
    with pytest.raises(ValueError, match="symbol"):
        record_hourly_pull(
            tmp_path / "hourly.json",
            symbol,
            datetime(2026, 11, 7, 19, 30),
        )


def test_record_rejects_invalid_datetime(tmp_path):
    with pytest.raises(ValueError, match="datetime"):
        record_hourly_pull(
            tmp_path / "hourly.json",
            "NVDA",
            "2026-11-07T19:30:00",
        )
