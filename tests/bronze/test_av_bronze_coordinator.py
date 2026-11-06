from unittest.mock import mock_open

from alphavantage_trackers.pipelines.queue import av_bronze_coordinator as coordinator


def test_coordinator_adds_stock_and_commodity_jobs(monkeypatch):

    # Mock configuration data
    mock_configs = {
        "alphavantage_stock_config.json": {
            "stocks": {
                "NVDA": {"function": "TIME_SERIES_DAILY"}
            }
        },
        "alphavantage_commodities_config.json": {
            "commodities": {
                "XAU": {"function": "GOLD_SILVER_HISTORY"}
            }
        }
    }

    # Mock file opening and JSON loading
    def mock_file_open(path, *args, **kwargs):
        return mock_open()()

    monkeypatch.setattr("builtins.open", mock_file_open)

    monkeypatch.setattr(
        coordinator.json,
        "load",
        lambda file: mock_configs[
            "alphavantage_stock_config.json"
            if "stock" in str(file.name)
            else "alphavantage_commodities_config.json"
        ]
    )

    # Capture jobs added to queue
    added_jobs = []

    class MockQueue:
        def add_jobs(self, asset_type, configs):
            added_jobs.append((asset_type, configs))

    monkeypatch.setattr(coordinator, "AlphaVantageQueue", MockQueue)

    # Mock worker execution
    worker_called = []

    def mock_worker(queue):
        worker_called.append(queue)
        return True

    monkeypatch.setattr(
        coordinator,
        "run_av_queue_worker",
        mock_worker
    )

    # Act
    result = coordinator.run_av_bronze_coordinator()

    # Assert
    assert result is True

    assert len(added_jobs) == 2
    assert added_jobs[0][0] == "stock"
    assert added_jobs[1][0] == "commodities"

    assert worker_called
    assert isinstance(worker_called[0], MockQueue)