
from datetime import date, datetime

import pytest

from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)
from market_data.pipelines.twelvedata.bronze import update


def test_recent_coverage_processes_all_pending_stocks(
    tmp_path,
    monkeypatch,
):
    calls = []

    stocks = [
        {"symbol": "NVDA", "start_date": "2020-01-01"},
        {"symbol": "AAPL", "start_date": "2020-01-01"},
    ]

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(update, "get_latest_stock_date", lambda *args: None)

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        lambda **kwargs: (
            calls.append(kwargs)
            or {
                "symbol": kwargs["symbol"],
                "success": True,
                "has_data": True,
                "rows": 5000,
            }
        ),
    )

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=tmp_path / "hourly.json",
        ingestion_state_path=tmp_path / "ingestion.json",
        today=date(2026, 11, 7),
    )

    assert len(results) == 2
    assert {call["symbol"] for call in calls} == {"NVDA", "AAPL"}


def test_backfill_starts_only_after_recent_coverage(
    tmp_path,
    monkeypatch,
):
    calls = []

    stocks = [
        {"symbol": "NVDA", "start_date": "2020-01-01"},
        {"symbol": "AAPL", "start_date": "2020-01-01"},
    ]

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(update, "get_latest_stock_date", lambda *args: None)
    monkeypatch.setattr(
        update,
        "get_oldest_stock_date",
        lambda *args: date(2025, 1, 1),
    )

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        lambda **kwargs: (
            calls.append(kwargs)
            or {
                "symbol": kwargs["symbol"],
                "success": True,
                "has_data": True,
                "rows": 5000,
                "earliest_date": "20240101",
            }
        ),
    )

    state_path = tmp_path / "ingestion.json"
    hourly_path = tmp_path / "hourly.json"

    update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=hourly_path,
        ingestion_state_path=state_path,
        today=date(2026, 11, 7),
        now=datetime(2026, 11, 7, 10, 0),
    )

    assert len(calls) == 2
    calls.clear()

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=hourly_path,
        ingestion_state_path=state_path,
        today=date(2026, 11, 7),
        now=datetime(2026, 11, 7, 11, 0),
    )

    assert len(results) == 2
    assert len(calls) == 2
    assert all(call["end_date"] == "2024-12-31" for call in calls)


def test_backfill_prioritises_shallowest_history(
    tmp_path,
    monkeypatch,
):
    calls = []

    stocks = [
        {"symbol": "NVDA", "start_date": "2020-01-01"},
        {"symbol": "AAPL", "start_date": "2020-01-01"},
    ]

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args: date(2026, 11, 7),
    )
    monkeypatch.setattr(
        update,
        "get_oldest_stock_date",
        lambda path, symbol: {
            "NVDA": date(2024, 1, 1),
            "AAPL": date(2025, 1, 1),
        }[symbol],
    )

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        lambda **kwargs: (
            calls.append(kwargs)
            or {
                "symbol": kwargs["symbol"],
                "success": True,
                "has_data": True,
                "rows": 5000,
                "earliest_date": "20240101",
            }
        ),
    )

    state_path = tmp_path / "ingestion.json"

    update.initialize_symbols(state_path, ["NVDA", "AAPL"])

    for symbol in ["NVDA", "AAPL"]:
        update.mark_recent_complete(state_path, symbol)

    update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=tmp_path / "hourly.json",
        ingestion_state_path=state_path,
        today=date(2026, 11, 7),
        now=datetime(2026, 11, 7, 10, 0),
    )

    assert calls[0]["symbol"] == "AAPL"
    assert calls[1]["symbol"] == "NVDA"


def test_quota_error_does_not_mark_backfill_complete(
    tmp_path,
    monkeypatch,
):
    stocks = [
        {"symbol": "NVDA", "start_date": "2020-01-01"},
    ]

    state_path = tmp_path / "ingestion.json"
    hourly_path = tmp_path / "hourly.json"

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args: date(2026, 11, 7),
    )
    monkeypatch.setattr(
        update,
        "get_oldest_stock_date",
        lambda *args: date(2025, 1, 1),
    )

    update.initialize_symbols(state_path, ["NVDA"])
    update.mark_recent_complete(state_path, "NVDA")

    def fake_ingest(**kwargs):
        raise TwelveDataDailyLimitError("Daily quota exhausted")

    monkeypatch.setattr(update, "ingest_stock_batch", fake_ingest)

    with pytest.raises(TwelveDataDailyLimitError):
        update.update_stocks(
            provider=object(),
            bronze_path=tmp_path / "bronze",
            state_path=hourly_path,
            ingestion_state_path=state_path,
            today=date(2026, 11, 7),
            now=datetime(2026, 11, 7, 10, 0),
        )

    symbol_state = update.get_symbol_state(state_path, "NVDA")

    assert symbol_state["recent_complete"] is True
    assert symbol_state["backfill_complete"] is False


def test_backfill_resumes_from_oldest_saved_date(
    tmp_path,
    monkeypatch,
):
    calls = []

    stocks = [
        {"symbol": "NVDA", "start_date": "2020-01-01"},
    ]

    state_path = tmp_path / "ingestion.json"

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args: date(2026, 11, 7),
    )

    oldest_dates = [
        date(2025, 1, 1),
        date(2024, 1, 1),
    ]

    def fake_oldest_date(*args):
        return oldest_dates.pop(0)

    monkeypatch.setattr(update, "get_oldest_stock_date", fake_oldest_date)

    update.initialize_symbols(state_path, ["NVDA"])
    update.mark_recent_complete(state_path, "NVDA")

    def fake_ingest(**kwargs):
        calls.append(kwargs)

        return {
            "symbol": "NVDA",
            "success": True,
            "has_data": True,
            "rows": 5000,
            "earliest_date": "20240101",
            "latest_date": "20241231",
        }

    monkeypatch.setattr(update, "ingest_stock_batch", fake_ingest)

    update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=tmp_path / "hourly.json",
        ingestion_state_path=state_path,
        today=date(2026, 11, 7),
        now=datetime(2026, 11, 7, 10, 0),
    )

    assert calls[0]["end_date"] == "2024-12-31"

    update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=tmp_path / "hourly.json",
        ingestion_state_path=state_path,
        today=date(2026, 11, 7),
        now=datetime(2026, 11, 7, 11, 0),
    )

    assert calls[1]["end_date"] == "2023-12-31"


def test_quota_interruption_resumes_remaining_backfill(
    tmp_path,
    monkeypatch,
):
    stocks = [
        {"symbol": "AAPL", "start_date": "2020-01-01"},
        {"symbol": "NVDA", "start_date": "2020-01-01"},
    ]

    ingestion_state = tmp_path / "ingestion.json"
    hourly_state = tmp_path / "hourly.json"

    oldest_dates = {
        "AAPL": date(2025, 1, 1),
        "NVDA": date(2024, 1, 1),
    }

    latest_date = date(2026, 11, 7)
    calls = []
    quota_exhausted = True

    monkeypatch.setattr(update, "load_stocks_config", lambda: stocks)
    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args: latest_date,
    )
    monkeypatch.setattr(
        update,
        "get_oldest_stock_date",
        lambda path, symbol: oldest_dates[symbol],
    )

    update.initialize_symbols(ingestion_state, ["AAPL", "NVDA"])

    for symbol in ["AAPL", "NVDA"]:
        update.mark_recent_complete(ingestion_state, symbol)

    def fake_ingest(**kwargs):
        nonlocal quota_exhausted

        symbol = kwargs["symbol"]
        calls.append((symbol, kwargs["end_date"]))

        if symbol == "NVDA" and quota_exhausted:
            raise TwelveDataDailyLimitError("Daily quota exhausted")

        if symbol == "AAPL":
            oldest_dates["AAPL"] = date(2023, 1, 1)
            earliest = "20230101"
        else:
            oldest_dates["NVDA"] = date(2022, 1, 1)
            earliest = "20220101"

        return {
            "symbol": symbol,
            "success": True,
            "has_data": True,
            "rows": 5000,
            "earliest_date": earliest,
            "latest_date": "20241231",
        }

    monkeypatch.setattr(update, "ingest_stock_batch", fake_ingest)

    # First run: AAPL succeeds, then NVDA hits the quota.
    with pytest.raises(TwelveDataDailyLimitError):
        update.update_stocks(
            provider=object(),
            bronze_path=tmp_path / "bronze",
            state_path=hourly_state,
            ingestion_state_path=ingestion_state,
            today=date(2026, 11, 7),
            now=datetime(2026, 11, 7, 10, 0),
        )

    assert calls == [
        ("AAPL", "2024-12-31"),
        ("NVDA", "2023-12-31"),
    ]

    assert update.get_symbol_state(
        ingestion_state,
        "AAPL",
    )["backfill_complete"] is False

    assert update.get_symbol_state(
        ingestion_state,
        "NVDA",
    )["backfill_complete"] is False

    # Simulate the quota resetting the following day.
    quota_exhausted = False
    calls.clear()
    latest_date = date(2026, 11, 8)

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=hourly_state,
        ingestion_state_path=ingestion_state,
        today=date(2026, 11, 8),
        now=datetime(2026, 11, 8, 10, 0),
    )

    assert calls[0] == ("NVDA", "2023-12-31")
    assert calls[1] == ("AAPL", "2022-12-31")

    assert len(results) == 2
    assert all(result["success"] for result in results)
