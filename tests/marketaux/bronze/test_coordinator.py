
import market_data.pipelines.marketaux.bronze.coordinator as coordinator
from unittest.mock import Mock
from market_data.storage.base import StorageBackend

def test_coordinator_calls_downloader(monkeypatch):
    calls = []

    expected_results = {
        "status": "success",
        "successful": [
            {"symbol": "NVDA", "page": 1, "path": "nvda.json"}
        ],
        "failed": [],
    }

    def fake_run_marketaux_bronze(
        client=None,
        state=None,
        stocks=None,
        storage=None,
    ):
        calls.append({
            "client": client,
            "state": state,
            "stocks": stocks,
            "storage": storage,
        })
        return expected_results

    monkeypatch.setattr(
        coordinator,
        "run_marketaux_bronze",
        fake_run_marketaux_bronze,
    )

    client = object()
    state = object()
    stocks = [{"symbol": "NVDA", "name": "NVIDIA"}]
    storage = Mock(spec=StorageBackend)

    result = coordinator.run_marketaux_bronze_coordinator(
        client=client,
        state=state,
        stocks=stocks,
        storage=storage,
    )

    assert result == expected_results
    assert calls == [{
        "client": client,
        "state": state,
        "stocks": stocks,
        "storage": storage,
    }]


def test_coordinator_returns_partial_results(monkeypatch):
    expected_results = {
        "status": "partial",
        "successful": [
            {"symbol": "NVDA", "page": 1, "path": "nvda.json"}
        ],
        "failed": [
            {"symbol": "MSFT", "error": "Request failed"}
        ],
    }

    monkeypatch.setattr(
        coordinator,
        "run_marketaux_bronze",
        lambda **kwargs: expected_results,
    )

    result = coordinator.run_marketaux_bronze_coordinator()

    assert result == expected_results


def test_coordinator_propagates_unexpected_errors(monkeypatch):
    def raise_error(**kwargs):
        raise RuntimeError("Unexpected failure")

    monkeypatch.setattr(
        coordinator,
        "run_marketaux_bronze",
        raise_error,
    )

    try:
        coordinator.run_marketaux_bronze_coordinator()
    except RuntimeError as error:
        assert str(error) == "Unexpected failure"
    else:
        raise AssertionError("Expected RuntimeError")
