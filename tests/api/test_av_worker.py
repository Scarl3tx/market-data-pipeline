from alphavantage_trackers.pipelines.queue.av_queue import AlphaVantageQueue
from alphavantage_trackers.pipelines.queue.av_worker import run_av_queue_worker
import pytest
from alphavantage_trackers.api.exceptions import AlphaVantageRateLimitError
def test_worker_completes_successful_stock_job(tmp_path, monkeypatch):
    # Arrange: create an isolated queue
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs(
        "stock",
        {
            "NVDA": {
                "function": "TIME_SERIES_DAILY",
                "symbol_param": "symbol",
                "schema_type": "equity_daily"
            }
        }
    )

    # Mock extraction and saving to avoid API calls and disk writes
    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.extract_raw_stock",
        lambda symbol, config: {"mock": "data"}
    )

    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.save_stock_to_disk",
        lambda symbol, data: True
    )

    # Act
    result = run_av_queue_worker(queue)

    # Assert
    assert result is True

    # The job should now be completed
    with queue._connect() as conn:
        job = conn.execute(
            "SELECT status FROM api_jobs WHERE symbol = ?",
            ("NVDA",)
        ).fetchone()

    assert job[0] == "completed"

def test_worker_retries_failed_stock_job(tmp_path, monkeypatch):
    monkeypatch.setattr(
    "alphavantage_trackers.pipelines.queue.av_worker.STOCKS_PATH",
    tmp_path / "stocks"
    )
    # Arrange: create an isolated queue
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs(
        "stock",
        {
            "NVDA": {
                "function": "TIME_SERIES_DAILY",
                "symbol_param": "symbol",
                "schema_type": "equity_daily"
            }
        }
    )

    # Simulate an extraction failure
    def mock_extract_failure(symbol, config):
        raise RuntimeError("Simulated extraction failure")

    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.extract_raw_stock",
        mock_extract_failure
    )

    # Act
    result = run_av_queue_worker(queue)

    # Assert
    assert result is False

    with queue._connect() as conn:
        job = conn.execute(
            "SELECT status, attempts, last_error FROM api_jobs WHERE symbol = ?",
            ("NVDA",)
        ).fetchone()

    assert job[0] == "failed"
    assert job[1] == 3
    assert job[2] == "Simulated extraction failure"

def test_worker_stops_on_rate_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(
    "alphavantage_trackers.pipelines.queue.av_worker.STOCKS_PATH",
    tmp_path / "stocks"
    )
    # Arrange: create an isolated queue
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs(
        "stock",
        {
            "NVDA": {
                "function": "TIME_SERIES_DAILY",
                "symbol_param": "symbol",
                "schema_type": "equity_daily"
            },
            "AMD": {
                "function": "TIME_SERIES_DAILY",
                "symbol_param": "symbol",
                "schema_type": "equity_daily"
            }
        }
    )

    # Simulate an API rate-limit response
    def mock_rate_limit(symbol, config):
        raise AlphaVantageRateLimitError("API rate limit exceeded")

    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.extract_raw_stock",
        mock_rate_limit
    )

    # Act
    result = run_av_queue_worker(queue)

    # Assert
    assert result is False

    with queue._connect() as conn:
        jobs = conn.execute(
            "SELECT symbol, status, attempts FROM api_jobs ORDER BY job_id"
        ).fetchall()

    # First job should be returned to pending
    assert jobs[0][1] == "pending"
    assert jobs[0][2] == 1

    # Second job should remain untouched
    assert jobs[1][1] == "pending"
    assert jobs[1][2] == 0

def test_worker_completes_successful_commodity_job(tmp_path, monkeypatch):
    # Arrange: create an isolated queue
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs(
        "commodities",
        {
            "XAU": {
                "function": "GOLD_SILVER_HISTORY",
                "symbol_param": "symbol",
                "additional_params": {"interval": "daily"},
                "schema_type": "commodity_gold"
            }
        }
    )

    # Mock commodity extraction and saving
    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.extract_raw_commodities",
        lambda symbol, config: {"mock": "commodity data"}
    )

    monkeypatch.setattr(
        "alphavantage_trackers.pipelines.queue.av_worker.save_commodities_to_disk",
        lambda symbol, data: True
    )

    # Act
    result = run_av_queue_worker(queue)

    # Assert
    assert result is True

    with queue._connect() as conn:
        job = conn.execute(
            "SELECT status FROM api_jobs WHERE symbol = ?",
            ("XAU",)
        ).fetchone()

    assert job[0] == "completed"

