from datetime import datetime, timedelta

from src.historical.ingestion.orchestrator import run_historical_ingestion


def test_historical_batches_move_backwards_without_overlap(tmp_path, monkeypatch):
    from src.historical.ingestion import orchestrator

    # Use an isolated progress file.
    monkeypatch.setattr(
        orchestrator,
        "HistoricalProgressTracker",
        lambda start_date, end_date: __import__(
            "src.historical.ingestion.progress_tracker",
            fromlist=["HistoricalProgressTracker"]
        ).HistoricalProgressTracker(
            start_date,
            end_date,
            progress_path=tmp_path / "progress.json"
        )
    )

    batch_calls = []

    def mock_ingest(provider, symbol, start_date, end_date):
        batch_calls.append((start_date, end_date))

        if len(batch_calls) == 1:
            # First batch: latest historical data.
            return {
                "success": True,
                "has_data": True,
                "earliest_date": "20200101",
                "latest_date": "20261102",
            }

        if len(batch_calls) == 2:
            # Second batch: older historical data.
            return {
                "success": True,
                "has_data": True,
                "earliest_date": "20100101",
                "latest_date": "20191231",
            }

        # No older data remains.
        return {
            "success": True,
            "has_data": False,
            "earliest_date": None,
            "latest_date": None,
        }

    monkeypatch.setattr(
        orchestrator,
        "ingest_historical_stock_batch",
        mock_ingest
    )

    run_historical_ingestion(
        provider=None,
        stock_symbols=["NVDA"],
        commodity_symbols=[],
        start_date="1999-01-01",
        end_date="2026-11-03",
    )

    assert len(batch_calls) == 3

    # First batch ends at the requested end date.
    assert batch_calls[0] == ("1999-01-01", "2026-11-03")

    # Second batch ends one calendar day before the first batch's earliest date.
    assert batch_calls[1][1] == "2019-12-31"

    # Third batch ends one calendar day before the second batch's earliest date.
    assert batch_calls[2][1] == "2009-12-31"