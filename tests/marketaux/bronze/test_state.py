
import json
from datetime import datetime, timedelta, timezone

import pytest

from market_data.pipelines.marketaux.bronze.state import (
    MarketauxNewsState,
    MarketauxStateError,
)


def test_initializes_new_stocks(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")

    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
    ])

    assert set(state.state["stocks"]) == {"NVDA", "AAPL"}

    stock = state.get_stock("NVDA")
    assert stock["last_attempt_at"] is None
    assert stock["last_successful_check"] is None
    assert stock["pending_since"] is None
    assert stock["pending_until"] is None
    assert stock["next_page"] is None


def test_preserves_existing_stock_progress(tmp_path):
    path = tmp_path / "state.json"

    path.write_text(json.dumps({
        "stocks": {
            "NVDA": {
                "name": "NVIDIA",
                "last_attempt_at": "2026-11-01T10:00:00+00:00",
                "last_successful_check": "2026-11-01T09:00:00+00:00",
            }
        }
    }), encoding="utf-8")

    state = MarketauxNewsState(path)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
    ])

    stock = state.get_stock("NVDA")

    assert stock["last_successful_check"] == (
        "2026-11-01T09:00:00+00:00"
    )
    assert stock["next_page"] is None
    assert stock["pending_until"] is None
    assert "AAPL" in state.state["stocks"]


def test_records_attempt(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.record_attempt("nvda")

    assert state.get_stock("NVDA")["last_attempt_at"] is not None


def test_starts_initial_ingestion_with_30_day_window(
    tmp_path, monkeypatch
):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    fixed_now = datetime(
        2026, 11, 6, 10, 0, tzinfo=timezone.utc
    )
    monkeypatch.setattr(state, "_now", lambda: fixed_now)

    state.start_ingestion("NVDA")

    stock = state.get_stock("NVDA")

    assert stock["pending_since"] == (
        fixed_now - timedelta(days=30)
    ).isoformat()
    assert stock["pending_until"] == fixed_now.isoformat()
    assert stock["next_page"] == 1
    assert stock["ingestion_in_progress"] is True


def test_starts_incremental_ingestion_with_overlap(
    tmp_path, monkeypatch
):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    checkpoint = datetime(
        2026, 11, 5, 10, 0, tzinfo=timezone.utc
    )
    fixed_now = datetime(
        2026, 11, 6, 10, 0, tzinfo=timezone.utc
    )

    state.state["stocks"]["NVDA"]["last_successful_check"] = (
        checkpoint.isoformat()
    )
    state._save()

    monkeypatch.setattr(state, "_now", lambda: fixed_now)

    state.start_ingestion("NVDA", overlap_hours=24)

    stock = state.get_stock("NVDA")

    assert stock["pending_since"] == (
        checkpoint - timedelta(hours=24)
    ).isoformat()
    assert stock["pending_until"] == fixed_now.isoformat()


def test_does_not_reset_window_when_resuming(
    tmp_path, monkeypatch
):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    first_now = datetime(
        2026, 11, 6, 10, 0, tzinfo=timezone.utc
    )
    monkeypatch.setattr(state, "_now", lambda: first_now)

    state.start_ingestion("NVDA")
    original = state.get_stock("NVDA")

    state.record_page_saved("NVDA", 1)

    later_now = datetime(
        2026, 11, 7, 10, 0, tzinfo=timezone.utc
    )
    monkeypatch.setattr(state, "_now", lambda: later_now)

    state.start_ingestion("NVDA")
    resumed = state.get_stock("NVDA")

    assert resumed["pending_since"] == original["pending_since"]
    assert resumed["pending_until"] == original["pending_until"]
    assert resumed["next_page"] == 2


def test_rejects_invalid_lookback_and_overlap(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    with pytest.raises(ValueError):
        state.start_ingestion(
            "NVDA",
            initial_lookback_days=0,
        )

    with pytest.raises(ValueError):
        state.start_ingestion(
            "NVDA",
            overlap_hours=-1,
        )


def test_records_saved_page(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.start_ingestion("NVDA")
    state.record_page_saved("NVDA", 1)

    assert state.get_stock("NVDA")["next_page"] == 2


def test_rejects_unexpected_page(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.start_ingestion("NVDA")

    with pytest.raises(MarketauxStateError):
        state.record_page_saved("NVDA", 2)


def test_completes_ingestion(tmp_path, monkeypatch):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    fixed_now = datetime(
        2026, 11, 6, 10, 0, tzinfo=timezone.utc
    )
    monkeypatch.setattr(state, "_now", lambda: fixed_now)

    state.start_ingestion("NVDA")
    pending_until = state.get_stock("NVDA")["pending_until"]

    state.record_page_saved("NVDA", 1)
    state.complete_ingestion("NVDA")

    stock = state.get_stock("NVDA")

    assert stock["last_successful_check"] == pending_until
    assert stock["pending_since"] is None
    assert stock["pending_until"] is None
    assert stock["next_page"] is None
    assert stock["ingestion_in_progress"] is False


def test_resumes_from_next_page(tmp_path):
    path = tmp_path / "state.json"

    state = MarketauxNewsState(path)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.start_ingestion("NVDA")
    state.record_page_saved("NVDA", 1)

    reloaded = MarketauxNewsState(path)

    assert reloaded.get_stock("NVDA")["next_page"] == 2
    assert reloaded.get_stock("NVDA")["pending_since"] is not None
    assert reloaded.get_stock("NVDA")["pending_until"] is not None


def test_rejects_uninitialized_stock(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")

    with pytest.raises(MarketauxStateError):
        state.record_attempt("NVDA")


def test_prioritizes_pending_ingestion(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
        {"symbol": "MSFT", "name": "Microsoft"},
    ])

    state.state["stocks"]["NVDA"]["last_successful_check"] = (
        "2026-11-05T10:00:00+00:00"
    )
    state.state["stocks"]["AAPL"]["pending_since"] = (
        "2026-11-04T10:00:00+00:00"
    )
    state.state["stocks"]["AAPL"]["pending_until"] = (
        "2026-11-06T10:00:00+00:00"
    )
    state.state["stocks"]["AAPL"]["next_page"] = 3
    state._save()

    priority = state.get_stocks_by_priority()

    assert [symbol for symbol, _ in priority] == [
        "AAPL",
        "MSFT",
        "NVDA",
    ]


def test_prioritizes_least_recently_attempted(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
        {"symbol": "MSFT", "name": "Microsoft"},
    ])

    state.state["stocks"]["NVDA"]["last_attempt_at"] = (
        "2026-11-06T10:00:00+00:00"
    )
    state.state["stocks"]["AAPL"]["last_attempt_at"] = (
        "2026-11-06T11:00:00+00:00"
    )
    state._save()

    priority = state.get_stocks_by_priority()

    assert [symbol for symbol, _ in priority] == [
        "MSFT",
        "NVDA",
        "AAPL",
    ]


def test_persists_state_between_instances(tmp_path):
    path = tmp_path / "state.json"

    state = MarketauxNewsState(path)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])
    state.record_attempt("NVDA")

    reloaded = MarketauxNewsState(path)

    assert (
        reloaded.get_stock("NVDA")["last_attempt_at"] is not None
    )


def test_rejects_invalid_json_state(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(MarketauxStateError):
        MarketauxNewsState(path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("last_attempt_at", "invalid-date"),
        ("last_successful_check", "invalid-date"),
        ("pending_since", "invalid-date"),
        ("pending_until", "invalid-date"),
    ],
)
def test_rejects_invalid_state_timestamps(
    tmp_path, field, value
):
    path = tmp_path / "state.json"

    path.write_text(json.dumps({
        "stocks": {
            "NVDA": {
                "name": "NVIDIA",
                field: value,
            }
        }
    }), encoding="utf-8")

    with pytest.raises(MarketauxStateError):
        MarketauxNewsState(path)


@pytest.mark.parametrize("next_page", [0, -1, 1.5, "2", True])
def test_rejects_invalid_next_page(tmp_path, next_page):
    path = tmp_path / "state.json"

    path.write_text(json.dumps({
        "stocks": {
            "NVDA": {
                "name": "NVIDIA",
                "next_page": next_page,
            }
        }
    }), encoding="utf-8")

    with pytest.raises(MarketauxStateError):
        MarketauxNewsState(path)


@pytest.mark.parametrize(
    "checkpoint",
    [
        {
            "pending_since": None,
            "pending_until": None,
            "next_page": 2,
        },
        {
            "pending_since": "2026-11-06T10:00:00+00:00",
            "pending_until": "2026-11-05T10:00:00+00:00",
            "next_page": 2,
        },
        {
            "pending_since": "2026-11-05T10:00:00+00:00",
            "pending_until": "2026-11-06T10:00:00+00:00",
            "next_page": None,
        },
    ],
)
def test_rejects_invalid_active_checkpoint(
    tmp_path, checkpoint
):
    path = tmp_path / "state.json"

    path.write_text(json.dumps({
        "stocks": {
            "NVDA": {
                "name": "NVIDIA",
                "ingestion_in_progress": True,
                **checkpoint,
            }
        }
    }), encoding="utf-8")

    with pytest.raises(MarketauxStateError):
        MarketauxNewsState(path)


def test_get_stock_returns_independent_copy(tmp_path):
    state = MarketauxNewsState(tmp_path / "state.json")
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    stock = state.get_stock("NVDA")
    stock["name"] = "Modified"
    stock["next_page"] = 99

    actual = state.get_stock("NVDA")

    assert actual["name"] == "NVIDIA"
    assert actual["next_page"] is None


def test_loads_legacy_state_with_missing_fields(tmp_path):
    path = tmp_path / "state.json"

    path.write_text(json.dumps({
        "stocks": {
            "NVDA": {
                "name": "NVIDIA",
                "last_successful_check": (
                    "2026-11-01T09:00:00+00:00"
                ),
            }
        }
    }), encoding="utf-8")

    state = MarketauxNewsState(path)
    stock = state.get_stock("NVDA")

    assert stock["last_attempt_at"] is None
    assert stock["pending_since"] is None
    assert stock["pending_until"] is None
    assert stock["next_page"] is None
    assert stock["ingestion_in_progress"] is False

def test_load_state_accepts_utf8_bom(tmp_path):
    import json

    from market_data.pipelines.marketaux.bronze.state import (
        MarketauxNewsState,
    )

    state_path = tmp_path / "marketaux_news_state.json"

    state_path.write_text(
        json.dumps({"stocks": {}}),
        encoding="utf-8-sig",
    )

    state = MarketauxNewsState(state_path)

    assert state.state == {"stocks": {}}