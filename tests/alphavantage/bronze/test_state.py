import json

import pytest

from market_data.pipelines.alphavantage.bronze.state import (
    load_news_cursor,
    save_news_cursor,
)


def test_load_news_cursor_returns_none_when_missing(tmp_path):
    state_path = tmp_path / "news_state.json"

    assert load_news_cursor(state_path) is None


def test_save_news_cursor_creates_state_file(tmp_path):
    state_path = tmp_path / "news_state.json"

    cursor = save_news_cursor(
        "20261106T123045",
        state_path,
    )

    assert cursor == "20261106T1231"

    with open(state_path, encoding="utf-8") as file:
        state = json.load(file)

    assert state["next_time_from"] == "20261106T1231"


def test_load_news_cursor_returns_saved_timestamp(tmp_path):
    state_path = tmp_path / "news_state.json"

    save_news_cursor("20261106T123045", state_path)

    assert load_news_cursor(state_path) == "20261106T1231"


def test_save_news_cursor_handles_minute_boundary(tmp_path):
    state_path = tmp_path / "news_state.json"

    cursor = save_news_cursor(
        "20261106T235959",
        state_path,
    )

    assert cursor == "20261107T0000"


def test_load_news_cursor_rejects_invalid_timestamp(tmp_path):
    state_path = tmp_path / "news_state.json"

    state_path.write_text(
        '{"next_time_from": "invalid"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_news_cursor(state_path)