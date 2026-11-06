
from src.historical.transformation import orchestrator


def test_load_stock_symbols(monkeypatch):
    mock_config = {
        "stocks": {
            "NVDA": {},
            "AAPL": {},
            "MSFT": {},
        }
    }

    monkeypatch.setattr(
        orchestrator,
        "load_stock_symbols",
        lambda: list(mock_config["stocks"].keys())
    )

    symbols = orchestrator.load_stock_symbols()

    assert symbols == ["NVDA", "AAPL", "MSFT"]


def test_historical_silver_pipeline_success(monkeypatch):
    expected_symbols = ["NVDA", "AAPL", "MSFT"]
    captured_symbols = []

    monkeypatch.setattr(
        orchestrator,
        "load_stock_symbols",
        lambda: expected_symbols
    )

    def mock_transform(symbols):
        captured_symbols.extend(symbols)
        return True

    monkeypatch.setattr(
        orchestrator,
        "transform_historical_stocks",
        mock_transform
    )

    result = orchestrator.run_historical_silver_pipeline()

    assert result is True
    assert captured_symbols == expected_symbols


def test_historical_silver_pipeline_failure(monkeypatch):
    monkeypatch.setattr(
        orchestrator,
        "load_stock_symbols",
        lambda: ["NVDA", "AAPL"]
    )

    monkeypatch.setattr(
        orchestrator,
        "transform_historical_stocks",
        lambda symbols: False
    )

    result = orchestrator.run_historical_silver_pipeline()

    assert result is False
