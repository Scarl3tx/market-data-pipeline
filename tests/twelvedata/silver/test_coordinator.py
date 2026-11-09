from market_data.pipelines.twelvedata.silver import coordinator
from market_data.storage.local import LocalStorage

def test_coordinator_processes_all_stocks(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"}
        ]
    )

    processed = []

    def mock_run_stock_silver(symbol):
        processed.append(symbol)
        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_silver",
        mock_run_stock_silver
    )

    results = coordinator.run_twelvedata_silver()

    assert processed == ["NVDA", "AAPL", "MSFT"]
    assert len(results) == 3


def test_coordinator_continues_after_stock_failure(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"}
        ]
    )

    processed = []

    def mock_run_stock_silver(symbol):
        processed.append(symbol)

        if symbol == "AAPL":
            return {
                "symbol": symbol,
                "success": False,
                "skipped": False,
                "error": "Test failure"
            }

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_silver",
        mock_run_stock_silver
    )

    results = coordinator.run_twelvedata_silver()

    assert processed == ["NVDA", "AAPL", "MSFT"]
    assert results[1]["success"] is False
    assert results[2]["success"] is True


def test_coordinator_handles_skipped_stocks(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [{"symbol": "NVDA"}]
    )

    monkeypatch.setattr(
        coordinator,
        "run_stock_silver",
        lambda symbol: {
            "symbol": symbol,
            "success": True,
            "skipped": True,
            "reason": "No Bronze data found"
        }
    )

    results = coordinator.run_twelvedata_silver()

    assert results[0]["success"] is True
    assert results[0]["skipped"] is True


def test_coordinator_handles_empty_config(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: []
    )

    results = coordinator.run_twelvedata_silver()

    assert results == []


def test_coordinator_passes_storage_to_stock_runner(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
        ],
    )

    storage = LocalStorage(tmp_path / "storage")
    received = []

    def mock_run_stock_silver(symbol, storage=None):
        received.append((symbol, storage))

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100,
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_silver",
        mock_run_stock_silver,
    )

    results = coordinator.run_twelvedata_silver(
        storage=storage,
    )

    assert len(results) == 2
    assert received == [
        ("NVDA", storage),
        ("AAPL", storage),
    ]
