from alphavantage_trackers.pipelines.queue.av_queue import AlphaVantageQueue


def test_queue_creation(tmp_path):
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    assert queue.db_path.exists()


def test_add_and_retrieve_job(tmp_path):
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    configs = {
        "NVDA": {
            "function": "TIME_SERIES_DAILY",
            "symbol_param": "symbol",
            "schema_type": "equity_daily"
        }
    }

    queue.add_jobs("stock", configs)

    job = queue.get_next_job()

    assert job is not None
    assert job["asset_type"] == "stock"
    assert job["symbol"] == "NVDA"

def test_job_is_claimed(tmp_path):
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    configs = {
        "NVDA": {
            "function": "TIME_SERIES_DAILY",
            "symbol_param": "symbol",
            "schema_type": "equity_daily"
        }
    }

    queue.add_jobs("stock", configs)

    first_job = queue.get_next_job()

    assert first_job["symbol"] == "NVDA"
    assert first_job["attempts"] == 1

    # The same job should not be returned again.
    second_job = queue.get_next_job()

    assert second_job is None

def test_complete_job(tmp_path):
        queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

        queue.add_jobs("stock", {"NVDA": {"function": "TIME_SERIES_DAILY"}})

        job = queue.get_next_job()

        assert queue.complete_job(job["job_id"]) is True

        assert queue.get_next_job() is None

def test_failed_job_can_be_retried(tmp_path):
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs("stock", {"NVDA": {"function": "TIME_SERIES_DAILY"}})

    job = queue.get_next_job()

    assert queue.fail_job(job["job_id"], "Test error") is True

    # Job should return to pending for another attempt.
    retry_job = queue.get_next_job()

    assert retry_job is not None
    assert retry_job["symbol"] == "NVDA"
    assert retry_job["attempts"] == 2

def test_job_fails_after_max_attempts(tmp_path):
    queue = AlphaVantageQueue(db_path=tmp_path / "test_queue.sqlite3")

    queue.add_jobs("stock", {"NVDA": {"function": "TIME_SERIES_DAILY"}})

    for attempt in range(3):
        job = queue.get_next_job()

        assert job is not None
        assert job["attempts"] == attempt + 1

        assert queue.fail_job(
            job["job_id"],
            "Test error",
            max_attempts=3
        ) is True

    # No pending jobs should remain.
    assert queue.get_next_job() is None