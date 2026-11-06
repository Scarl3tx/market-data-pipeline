
from src.historical.ingestion import orchestrator
from src.historical.ingestion.exceptions import (
    TwelveDataDailyLimitError
)
from src.historical.ingestion.progress_tracker import (
    HistoricalProgressTracker
)


def test_historical_ingestion_resumes_after_quota_exhaustion(
    tmp_path,
    monkeypatch
):
    progress_path = tmp_path / "progress.json"

    # Use the same persistent temporary progress file across executions
    class TestProgressTracker(HistoricalProgressTracker):
        def __init__(self, start_date, end_date):
            super().__init__(
                start_date=start_date,
                end_date=end_date,
                progress_path=progress_path
            )

    monkeypatch.setattr(
        orchestrator,
        "HistoricalProgressTracker",
        TestProgressTracker
    )

    calls = []
    quota_exhausted = False

    def mock_ingest(provider, symbol, start_date, end_date):
        nonlocal quota_exhausted

        calls.append((symbol, end_date))

        if symbol == "MSFT" and not quota_exhausted:
            quota_exhausted = True
            raise TwelveDataDailyLimitError("Daily quota exhausted")

        # Simulate no more historical data on the second execution
        if len(calls) > 3:
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
            "earliest_date": "20200101",
            "latest_date": "20261103",
            "has_data": True
        }