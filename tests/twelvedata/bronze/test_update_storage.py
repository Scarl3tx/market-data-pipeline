
from datetime import date, datetime

from market_data.storage.local import LocalStorage

from market_data.pipelines.twelvedata.bronze import update


def test_update_stocks_uses_storage_for_checkpoints(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")

    monkeypatch.setattr(
        update,
        "load_stocks_config",
        lambda: [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args, **kwargs: None,
    )

    def fake_ingest(**kwargs):
        assert kwargs["storage"] is storage

        return {
            "symbol": "NVDA",
            "success": True,
            "has_data": True,
            "rows": 10,
            "earliest_date": "20260101",
            "latest_date": "20261108",
        }

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    results = update.update_stocks(
        provider=object(),
        storage=storage,
        today=date(2026, 11, 8),
        now=datetime(2026, 11, 8, 20, 0),
    )

    assert results[0]["success"] is True

    assert storage.exists(
        "state/twelvedata/hourly_state.json"
    )

    assert storage.exists(
        "state/twelvedata/ingestion_state.json"
    )

    ingestion_state = storage.read_json(
        "state/twelvedata/ingestion_state.json"
    )

    assert ingestion_state["symbols"]["NVDA"] == {
        "recent_complete": True,
        "backfill_complete": False,
    }

    hourly_state = storage.read_json(
        "state/twelvedata/hourly_state.json"
    )

    assert "NVDA" in hourly_state


def test_update_stocks_resumes_from_storage_checkpoint(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")

    monkeypatch.setattr(
        update,
        "load_stocks_config",
        lambda: [
            {
                "symbol": "NVDA",
                "start_date": "2026-01-01",
            }
        ],
    )

    state_key = "state/twelvedata/ingestion_state.json"

    storage.write_json(
        state_key,
        {
            "symbols": {
                "NVDA": {
                    "recent_complete": True,
                    "backfill_complete": False,
                }
            }
        },
    )

    monkeypatch.setattr(
        update,
        "get_oldest_stock_date",
        lambda *args, **kwargs: date(2026, 6, 1),
    )

    monkeypatch.setattr(
        update,
        "get_latest_stock_date",
        lambda *args, **kwargs: date(2026, 11, 6),
    )

    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs)

        return {
            "symbol": "NVDA",
            "success": True,
            "has_data": True,
            "rows": 10,
            "earliest_date": "20260531",
            "latest_date": "20260531",
        }

    monkeypatch.setattr(
        update,
        "ingest_stock_batch",
        fake_ingest,
    )

    results = update.update_stocks(
        provider=object(),
        storage=storage,
        today=date(2026, 11, 8),
        now=datetime(2026, 11, 8, 20, 0),
    )

    assert len(calls) == 1
    assert calls[0]["storage"] is storage
    assert calls[0]["start_date"] == "2026-01-01"
    assert calls[0]["end_date"] == "2026-05-31"

    assert results[0]["success"] is True
