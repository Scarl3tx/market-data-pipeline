
from datetime import datetime, date

import pytest

from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)

from market_data.pipelines.twelvedata.bronze import update


def _configure_stocks(monkeypatch, stocks):
    monkeypatch.setattr(
        update,
        "load_stocks_config",
        lambda: stocks,
    )


def test_skips_stock_already_pulled_this_hour(
    tmp_path,
    monkeypatch,
):
    calls = []

    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return {"symbol": "NVDA", "success": True}

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    now = datetime(2026, 11, 6, 19, 30)
    state_path = tmp_path / "state" / "hourly.json"

    update.record_hourly_pull(
        state_path,
        "NVDA",
        now,
    )

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=state_path,
        today=date(2026, 11, 6),
        now=datetime(2026, 11, 6, 19, 45),
    )

    assert results[0]["skipped"] is True
    assert results[0]["reason"] == "Already pulled this hour"
    assert calls == []


def test_allows_stock_to_be_pulled_next_hour(
    tmp_path,
    monkeypatch,
):
    calls = []

    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return {"symbol": "NVDA", "success": True}

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    state_path = tmp_path / "state" / "hourly.json"

    update.record_hourly_pull(
        state_path,
        "NVDA",
        datetime(2026, 11, 6, 19, 30),
    )

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=state_path,
        today=date(2026, 11, 6),
        now=datetime(2026, 11, 6, 20, 5),
    )

    assert len(calls) == 1
    assert results[0]["success"] is True


def test_failed_ingestion_does_not_record_hourly_pull(
    tmp_path,
    monkeypatch,
):
    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        lambda **kwargs: {
            "symbol": "NVDA",
            "success": False,
            "error": "API failure",
        },
    )

    state_path = tmp_path / "state" / "hourly.json"
    now = datetime(2026, 11, 6, 19, 30)

    update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=state_path,
        today=date(2026, 11, 6),
        now=now,
    )

    assert update.has_pulled_this_hour(
        state_path,
        "NVDA",
        now,
    ) is False


def test_unexpected_failure_does_not_stop_remaining_stocks(
    tmp_path,
    monkeypatch,
):
    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "AAA",
                "start_date": "2026-01-01",
            },
            {
                "symbol": "BBB",
                "start_date": "2026-01-01",
            },
        ],
    )

    calls = []

    def fake_ingest(**kwargs):
        symbol = kwargs["symbol"]
        calls.append(symbol)

        if symbol == "AAA":
            raise RuntimeError("Unexpected provider failure")

        return {
            "symbol": symbol,
            "success": True,
            "rows": 10,
            "has_data": True,
            "earliest_date": "20261105",
            "latest_date": "20261106",
        }

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=tmp_path / "hourly.json",
        ingestion_state_path=tmp_path / "ingestion.json",
        today=date(2026, 11, 6),
        now=datetime(2026, 11, 6, 19, 30),
    )

    assert calls == ["AAA", "BBB"]

    assert len(results) == 2

    assert results[0]["symbol"] == "AAA"
    assert results[0]["success"] is False
    assert "Unexpected provider failure" in results[0]["error"]

    assert results[1]["symbol"] == "BBB"
    assert results[1]["success"] is True


def test_unexpected_failure_does_not_record_hourly_pull(
    tmp_path,
    monkeypatch,
):
    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("Unexpected failure")
        ),
    )

    state_path = tmp_path / "hourly.json"
    now = datetime(2026, 11, 6, 19, 30)

    results = update.update_stocks(
        provider=object(),
        bronze_path=tmp_path / "bronze",
        state_path=state_path,
        today=date(2026, 11, 6),
        now=now,
    )

    assert results[0]["success"] is False

    assert update.has_pulled_this_hour(
        state_path,
        "NVDA",
        now,
    ) is False


def test_daily_limit_still_propagates(
    tmp_path,
    monkeypatch,
):
    _configure_stocks(
        monkeypatch,
        [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            },
            {
                "symbol": "AMD",
                "start_date": "2026-01-01",
            },
        ],
    )

    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs["symbol"])
        raise TwelveDataDailyLimitError(
            "Daily limit reached"
        )

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    with pytest.raises(TwelveDataDailyLimitError):
        update.update_stocks(
            provider=object(),
            bronze_path=tmp_path / "bronze",
            state_path=tmp_path / "hourly.json",
            today=date(2026, 11, 6),
            now=datetime(2026, 11, 6, 19, 30),
        )

    assert calls == ["AMD"]
