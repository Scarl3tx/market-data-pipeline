
from datetime import datetime

from src.historical.ingestion.orchestrator import (
    run_historical_ingestion
)

def test_historical_ingestion_processes_stocks_breadth_first(
    tmp_path,
    monkeypatch
):
    from src.historical.ingestion import orchestrator

    # Use an isolated progress file
    class TestProgressTracker:
        def __init__(self, start_date, end_date):
            from src.historical.ingestion.progress_tracker import (
                HistoricalProgressTracker
            )

            self.tracker = HistoricalProgressTracker(
                start_date=start_date,
                end_date=end_date,
                progress_path=tmp_path / "progress.json"
            )

        def __getattr__(self, name):
            return getattr(self.tracker, name)

    monkeypatch.setattr(
        orchestrator,
        "HistoricalProgressTracker",
        TestProgressTracker
    )

    calls = []

    # Each stock gets two batches, then completes
    batch_responses = {
        "NVDA": [
            "20200101",
            "20100101",
            None
        ],
        "AAPL": [
            "20200101",
            "20100101",
            None
        ],
        "MSFT": [
            "20200101",
            "20100101",
            None
        ]
    }

    def mock_ingest(
        provider,
        symbol,
        start_date,
        end_date
    ):
        calls.append(symbol)

        earliest_date = batch_responses[symbol].pop(0)

        if earliest_date is None:
            return {
                "symbol": symbol,
                "success": True,
                "rows": 0,
                "earliest_date": None,
                "latest_date": None,
                "has_data": False
            }

        return {
            "symbol": symbol,
            "success": True,
            "rows": 5000,
            "earliest_date": earliest_date,
            "latest_date": "20261103",
            "has_data": True
        }

    monkeypatch.setattr(
        orchestrator,
        "ingest_historical_stock_batch",
        mock_ingest
    )

    run_historical_ingestion(
        provider=None,
        stock_symbols=["NVDA", "AAPL", "MSFT"],
        commodity_symbols=[],
        start_date="1999-01-01",
        end_date="2026-11-03"
    )

    # Verify breadth-first processing
    assert calls == [
        "NVDA", "AAPL", "MSFT",
        "NVDA", "AAPL", "MSFT",
        "NVDA", "AAPL", "MSFT"
    ]
