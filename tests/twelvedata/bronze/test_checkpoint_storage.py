
from datetime import datetime

from market_data.storage.local import LocalStorage

from market_data.pipelines.twelvedata.bronze.state import (
    initialize_symbols,
    get_symbol_state,
    mark_recent_complete,
    mark_backfill_complete,
)

from market_data.pipelines.twelvedata.bronze.hourly_tracker import (
    has_pulled_this_hour,
    record_hourly_pull,
)


def test_ingestion_state_persists_through_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    state_key = "state/twelvedata/ingestion_state.json"

    initialize_symbols(
        state_key,
        ["NVDA", "AMD"],
        storage=storage,
    )

    mark_recent_complete(
        state_key,
        "NVDA",
        storage=storage,
    )

    mark_backfill_complete(
        state_key,
        "NVDA",
        storage=storage,
    )

    # Read using a new backend instance to verify persistence.
    reopened_storage = LocalStorage(tmp_path / "storage")

    assert get_symbol_state(
        state_key,
        "NVDA",
        storage=reopened_storage,
    ) == {
        "recent_complete": True,
        "backfill_complete": True,
    }

    assert get_symbol_state(
        state_key,
        "AMD",
        storage=reopened_storage,
    ) == {
        "recent_complete": False,
        "backfill_complete": False,
    }


def test_ingestion_state_preserves_existing_checkpoints(tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    state_key = "state/twelvedata/ingestion_state.json"

    initialize_symbols(
        state_key,
        ["NVDA"],
        storage=storage,
    )

    mark_recent_complete(
        state_key,
        "NVDA",
        storage=storage,
    )

    initialize_symbols(
        state_key,
        ["NVDA", "AMD"],
        storage=storage,
    )

    assert get_symbol_state(
        state_key,
        "NVDA",
        storage=storage,
    )["recent_complete"] is True

    assert get_symbol_state(
        state_key,
        "AMD",
        storage=storage,
    )["recent_complete"] is False


def test_hourly_tracker_persists_through_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    state_key = "state/twelvedata/hourly_state.json"

    now = datetime(2026, 11, 8, 10, 30)

    record_hourly_pull(
        state_key,
        "NVDA",
        now,
        storage=storage,
    )

    reopened_storage = LocalStorage(tmp_path / "storage")

    assert has_pulled_this_hour(
        state_key,
        "NVDA",
        datetime(2026, 11, 8, 10, 55),
        storage=reopened_storage,
    ) is True


def test_hourly_tracker_keeps_symbols_independent(tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    state_key = "state/twelvedata/hourly_state.json"

    now = datetime(2026, 11, 8, 10, 30)

    record_hourly_pull(
        state_key,
        "NVDA",
        now,
        storage=storage,
    )

    assert has_pulled_this_hour(
        state_key,
        "NVDA",
        now,
        storage=storage,
    ) is True

    assert has_pulled_this_hour(
        state_key,
        "AMD",
        now,
        storage=storage,
    ) is False


def test_hourly_tracker_allows_next_hour_with_storage(tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    state_key = "state/twelvedata/hourly_state.json"

    record_hourly_pull(
        state_key,
        "NVDA",
        datetime(2026, 11, 8, 10, 30),
        storage=storage,
    )

    assert has_pulled_this_hour(
        state_key,
        "NVDA",
        datetime(2026, 11, 8, 11, 5),
        storage=storage,
    ) is False
