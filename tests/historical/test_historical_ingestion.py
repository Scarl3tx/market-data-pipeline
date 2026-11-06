from src.historical.ingestion.progress_tracker import (
    HistoricalProgressTracker
)


def test_progress_tracker_persists_and_resumes(tmp_path):

    progress_path = tmp_path / "historical_progress.json"

    tracker = HistoricalProgressTracker(
        start_date="1999-01-01",
        end_date="2026-11-03",
        progress_path=progress_path
    )

    tracker.initialise_stock("TEST")

    assert tracker.get_stock_progress("TEST")["next_end_date"] == "2026-11-03"

    tracker.update_stock(
        symbol="TEST",
        next_end_date="2010-01-01"
    )

    # Simulate a new execution
    resumed_tracker = HistoricalProgressTracker(
        start_date="1999-01-01",
        end_date="2026-11-03",
        progress_path=progress_path
    )

    resumed_tracker.initialise_stock("TEST")

    assert resumed_tracker.get_stock_progress("TEST")["next_end_date"] == "2010-01-01"