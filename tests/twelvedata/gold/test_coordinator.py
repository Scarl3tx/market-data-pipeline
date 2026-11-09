
from market_data.pipelines.twelvedata.gold import coordinator


def test_coordinator_processes_all_stocks(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ],
    )

    processed = []

    def mock_run_stock_gold(symbol):
        processed.append(symbol)

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100,
            "rejected_rows": 0,
            "validation_reasons": {},
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        mock_run_stock_gold,
    )

    results = coordinator.run_twelvedata_gold()

    assert processed == ["NVDA", "AAPL", "MSFT"]
    assert len(results) == 3


def test_coordinator_continues_after_stock_failure(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ],
    )

    processed = []

    def mock_run_stock_gold(symbol):
        processed.append(symbol)

        if symbol == "AAPL":
            return {
                "symbol": symbol,
                "success": False,
                "skipped": False,
                "error": "Test failure",
            }

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100,
            "rejected_rows": 0,
            "validation_reasons": {},
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        mock_run_stock_gold,
    )

    results = coordinator.run_twelvedata_gold()

    assert processed == ["NVDA", "AAPL", "MSFT"]
    assert results[1]["success"] is False
    assert results[2]["success"] is True


def test_coordinator_handles_skipped_stocks(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [{"symbol": "NVDA"}],
    )

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        lambda symbol: {
            "symbol": symbol,
            "success": True,
            "skipped": True,
            "reason": "No Silver data found",
        },
    )

    results = coordinator.run_twelvedata_gold()

    assert results[0]["success"] is True
    assert results[0]["skipped"] is True


def test_coordinator_handles_empty_config(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [],
    )

    results = coordinator.run_twelvedata_gold()

    assert results == []


def test_coordinator_filters_requested_symbols(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ],
    )

    processed = []

    def mock_run_stock_gold(symbol):
        processed.append(symbol)

        return {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 10,
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        mock_run_stock_gold,
    )

    results = coordinator.run_twelvedata_gold(
        symbols={"NVDA", "MSFT"},
    )

    assert processed == ["NVDA", "MSFT"]
    assert len(results) == 2


def test_coordinator_aggregates_validation_statistics(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ],
    )

    def mock_run_stock_gold(symbol):
        results = {
            "NVDA": {
                "symbol": "NVDA",
                "success": True,
                "skipped": False,
                "rows": 100,
                "rejected_rows": 2,
                "validation_reasons": {
                    "invalid_volume": 2,
                },
            },
            "AAPL": {
                "symbol": "AAPL",
                "success": True,
                "skipped": False,
                "rows": 80,
                "rejected_rows": 3,
                "validation_reasons": {
                    "invalid_open": 1,
                    "invalid_volume": 2,
                },
            },
            "MSFT": {
                "symbol": "MSFT",
                "success": False,
                "skipped": False,
                "error": "Test failure",
            },
        }

        return results[symbol]

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        mock_run_stock_gold,
    )

    logged = []

    monkeypatch.setattr(
        coordinator.logger,
        "info",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "warning",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "error",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    results = coordinator.run_twelvedata_gold()

    assert len(results) == 3

    assert any(
        "Successful: 2 | Failed: 1 | Skipped: 0" in message
        for message in logged
    )

    assert any(
        "Rows loaded: 180 | Rows rejected: 5" in message
        for message in logged
    )

    assert any(
        "'invalid_open': 1" in message
        and "'invalid_volume': 4" in message
        for message in logged
    )


def test_coordinator_aggregates_skipped_and_empty_results(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
        ],
    )

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        lambda symbol: {
            "symbol": symbol,
            "success": True,
            "skipped": True,
            "reason": "No Silver data found",
        },
    )

    logged = []

    monkeypatch.setattr(
        coordinator.logger,
        "info",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "warning",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "error",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    results = coordinator.run_twelvedata_gold()

    assert len(results) == 2

    assert any(
        "Successful: 0 | Failed: 0 | Skipped: 2" in message
        for message in logged
    )

    assert any(
        "Rows loaded: 0 | Rows rejected: 0" in message
        for message in logged
    )


def test_coordinator_aggregates_quarantine_statistics(monkeypatch):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ],
    )

    def mock_run_stock_gold(symbol):
        results = {
            "NVDA": {
                "symbol": "NVDA",
                "success": True,
                "skipped": False,
                "rows": 100,
                "rejected_rows": 3,
                "quarantined_rows": 1,
                "validation_reasons": {
                    "invalid_volume": 3,
                },
            },
            "AAPL": {
                "symbol": "AAPL",
                "success": True,
                "skipped": False,
                "rows": 80,
                "rejected_rows": 2,
                "quarantined_rows": 0,
                "quarantine_error": "Disk write failed",
                "validation_reasons": {
                    "invalid_open": 2,
                },
            },
            "MSFT": {
                "symbol": "MSFT",
                "success": True,
                "skipped": False,
                "rows": 50,
                "rejected_rows": 0,
                "quarantined_rows": 0,
                "validation_reasons": {},
            },
        }

        return results[symbol]

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        mock_run_stock_gold,
    )

    logged = []

    monkeypatch.setattr(
        coordinator.logger,
        "info",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "warning",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "error",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    results = coordinator.run_twelvedata_gold()

    assert len(results) == 3

    assert any(
        "Successfully quarantined: 3" in message
        and "Failed to quarantine: 2" in message
        and "Symbols with quarantine failures: 1" in message
        for message in logged
    )


def test_coordinator_counts_repeated_rejections_as_quarantined(
    monkeypatch,
):
    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [{"symbol": "NVDA"}],
    )

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        lambda symbol: {
            "symbol": symbol,
            "success": True,
            "skipped": False,
            "rows": 100,
            "rejected_rows": 4,
            "quarantined_rows": 0,
            "validation_reasons": {},
        },
    )

    logged = []

    monkeypatch.setattr(
        coordinator.logger,
        "info",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "warning",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    monkeypatch.setattr(
        coordinator.logger,
        "error",
        lambda message, *args: logged.append(
            message % args if args else message
        ),
    )

    coordinator.run_twelvedata_gold()

    assert any(
        "Successfully quarantined: 4" in message
        and "Failed to quarantine: 0" in message
        for message in logged
    )
