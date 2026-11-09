
from datetime import datetime, timezone

from market_data.pipelines.marketaux.bronze.state import (
    MarketauxNewsState,
)


def test_recovers_incomplete_ingestion_checkpoint(
    tmp_path,
    monkeypatch,
):
    state = MarketauxNewsState(
        tmp_path / "marketaux_news_state.json"
    )

    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    stock = state.state["stocks"]["NVDA"]

    stock["ingestion_in_progress"] = True
    stock["pending_since"] = None
    stock["pending_until"] = None
    stock["next_page"] = 32

    fixed_now = datetime(
        2026, 11, 6, 15, 48, 0,
        tzinfo=timezone.utc,
    )

    monkeypatch.setattr(
        state,
        "_now",
        lambda: fixed_now,
    )

    state.start_ingestion("NVDA")

    recovered = state.get_stock("NVDA")

    assert recovered["ingestion_in_progress"] is True
    assert recovered["next_page"] == 1
    assert recovered["pending_since"] == (
        "2026-10-07T15:48:00+00:00"
    )
    assert recovered["pending_until"] == (
        "2026-11-06T15:48:00+00:00"
    )


def test_preserves_valid_pending_checkpoint(
    tmp_path,
    monkeypatch,
):
    state = MarketauxNewsState(
        tmp_path / "marketaux_news_state.json"
    )

    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    stock = state.state["stocks"]["NVDA"]

    stock["ingestion_in_progress"] = True
    stock["pending_since"] = "2026-10-07T00:00:00+00:00"
    stock["pending_until"] = "2026-11-06T00:00:00+00:00"
    stock["next_page"] = 32

    state.start_ingestion("NVDA")

    recovered = state.get_stock("NVDA")

    assert recovered["next_page"] == 32
    assert recovered["pending_since"] == (
        "2026-10-07T00:00:00+00:00"
    )
    assert recovered["pending_until"] == (
        "2026-11-06T00:00:00+00:00"
    )
