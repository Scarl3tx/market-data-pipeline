import pytest

from market_data.pipelines.twelvedata.bronze import coordinator
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError
)


def test_coordinator_runs_update(monkeypatch):
    expected_results = [
        {"symbol": "NVDA", "success": True, "rows": 100}
    ]

    monkeypatch.setattr(
        coordinator,
        "update_stocks",
        lambda provider: expected_results
    )

    results = coordinator.run_twelvedata_bronze(
        provider=object()
    )

    assert results == expected_results


def test_coordinator_creates_provider_when_none(monkeypatch):
    class MockClient:
        pass

    captured = {}

    def mock_client():
        client = MockClient()
        captured["client"] = client
        return client

    def mock_update(provider):
        captured["provider"] = provider
        return []

    monkeypatch.setattr(
        coordinator,
        "TwelveDataClient",
        mock_client
    )

    monkeypatch.setattr(
        coordinator,
        "update_stocks",
        mock_update
    )

    coordinator.run_twelvedata_bronze()

    assert captured["provider"] is captured["client"]


def test_daily_limit_propagates(monkeypatch):
    def mock_update(provider):
        raise TwelveDataDailyLimitError("Daily limit reached")

    monkeypatch.setattr(
        coordinator,
        "update_stocks",
        mock_update
    )

    with pytest.raises(TwelveDataDailyLimitError):
        coordinator.run_twelvedata_bronze(
            provider=object()
        )


def test_failed_stock_does_not_crash_coordinator(monkeypatch):
    expected_results = [
        {"symbol": "NVDA", "success": False, "error": "API error"},
        {"symbol": "AMD", "success": True, "rows": 100}
    ]

    monkeypatch.setattr(
        coordinator,
        "update_stocks",
        lambda provider: expected_results
    )

    results = coordinator.run_twelvedata_bronze(
        provider=object()
    )

    assert len(results) == 2
    assert results[0]["success"] is False
    assert results[1]["success"] is True