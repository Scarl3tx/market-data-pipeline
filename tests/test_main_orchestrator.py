import asyncio
import threading

from market_data.pipelines import main
import pytest


@pytest.fixture(autouse=True)
def disable_run_history(monkeypatch):
    monkeypatch.setattr(
        main,
        "save_pipeline_run",
        lambda results: results["run_id"],
    )

def test_providers_run_concurrently_and_combined_gold_runs_last(
    monkeypatch,
):
    barrier = threading.Barrier(2)
    completed = set()
    lock = threading.Lock()
    combined_called = []

    def fake_twelvedata():
        barrier.wait(timeout=5)

        with lock:
            completed.add("twelvedata")

        return {
            "bronze": [{"success": True}],
            "silver": [{"success": True}],
            "gold": [{"success": True}],
        }

    def fake_marketaux():
        barrier.wait(timeout=5)

        with lock:
            completed.add("marketaux")

        return {
            "bronze": {"status": "success"},
            "silver": {"status": "success"},
            "gold": {"status": "success"},
        }

    def fake_combined_gold():
        with lock:
            assert completed == {
                "twelvedata",
                "marketaux",
            }

        combined_called.append(True)
        return 100

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        fake_twelvedata,
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        fake_marketaux,
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        fake_combined_gold,
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["twelvedata"]["status"] == "success"
    assert results["marketaux"]["status"] == "success"

    assert results["twelvedata"]["result"] == {
        "bronze": [{"success": True}],
        "silver": [{"success": True}],
        "gold": [{"success": True}],
    }

    assert results["marketaux"]["result"] == {
        "bronze": {"status": "success"},
        "silver": {"status": "success"},
        "gold": {"status": "success"},
    }

    assert results["combined_gold"]["status"] == "success"
    assert results["combined_gold"]["rows"] == 100

    assert combined_called == [True]

def test_provider_exception_does_not_stop_other_provider(
    monkeypatch,
):
    combined_called = []

    def fake_twelvedata():
        raise RuntimeError("TwelveData failed")

    def fake_marketaux():
        return {
            "bronze": {"status": "success"},
            "silver": {"status": "success"},
            "gold": {"status": "success"},
        }

    def fake_combined_gold():
        combined_called.append(True)
        return 50

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        fake_twelvedata,
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        fake_marketaux,
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        fake_combined_gold,
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["twelvedata"]["status"] == "failed"
    assert "TwelveData failed" in results["twelvedata"]["error"]
    assert results["twelvedata"]["result"] is None

    assert results["marketaux"]["status"] == "success"

    assert results["combined_gold"]["status"] == "success"
    assert results["combined_gold"]["rows"] == 50

    assert combined_called == [True]

def test_combined_gold_runs_even_when_both_providers_fail(
    monkeypatch,
):
    combined_called = []

    def fake_twelvedata():
        raise RuntimeError("TwelveData unavailable")

    def fake_marketaux():
        raise RuntimeError("Marketaux unavailable")

    def fake_combined_gold():
        combined_called.append(True)
        return 25

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        fake_twelvedata,
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        fake_marketaux,
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        fake_combined_gold,
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["twelvedata"]["status"] == "failed"
    assert results["marketaux"]["status"] == "failed"

    assert results["combined_gold"]["status"] == "success"
    assert results["combined_gold"]["rows"] == 25

    assert combined_called == [True]

def test_provider_reports_partial_failure(
    monkeypatch,
):
    def fake_twelvedata():
        return {
            "bronze": [
                {"symbol": "NVDA", "success": True},
                {"symbol": "AAPL", "success": True},
                {"symbol": "MSFT", "success": False},
            ],
            "silver": [
                {"symbol": "NVDA", "success": True},
                {"symbol": "AAPL", "success": True},
            ],
            "gold": [
                {"symbol": "NVDA", "success": True},
                {"symbol": "AAPL", "success": True},
            ],
        }

    def fake_marketaux():
        return {
            "bronze": {"status": "success"},
            "silver": {"status": "success"},
            "gold": {"status": "success"},
        }

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        fake_twelvedata,
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        fake_marketaux,
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        lambda: 100,
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["twelvedata"]["status"] == "partial_failure"

    assert results["twelvedata"]["stages"]["bronze"] == (
        "partial_failure"
    )

    assert results["twelvedata"]["stages"]["silver"] == "success"
    assert results["twelvedata"]["stages"]["gold"] == "success"

    assert results["marketaux"]["status"] == "success"
    assert results["combined_gold"]["status"] == "success"

def test_combined_gold_failure_does_not_change_provider_results(
    monkeypatch,
):
    def fake_twelvedata():
        return {
            "bronze": [{"success": True}],
            "silver": [{"success": True}],
            "gold": [{"success": True}],
        }

    def fake_marketaux():
        return {
            "bronze": {"status": "success"},
            "silver": {"status": "success"},
            "gold": {"status": "success"},
        }

    def fake_combined_gold():
        raise RuntimeError("DuckDB write failed")

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        fake_twelvedata,
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        fake_marketaux,
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        fake_combined_gold,
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["twelvedata"]["status"] == "success"
    assert results["marketaux"]["status"] == "success"

    assert results["combined_gold"]["status"] == "failed"
    assert "DuckDB write failed" in results["combined_gold"]["error"]

def test_pipeline_skips_when_execution_lock_is_unavailable(
    monkeypatch,
):
    from contextlib import contextmanager

    provider_calls = []

    @contextmanager
    def unavailable_lock():
        yield False

    monkeypatch.setattr(
        main,
        "pipeline_execution_lock",
        unavailable_lock,
    )

    monkeypatch.setattr(
        main,
        "run_twelvedata_pipeline",
        lambda: provider_calls.append("twelvedata"),
    )

    monkeypatch.setattr(
        main,
        "run_marketaux_pipeline",
        lambda: provider_calls.append("marketaux"),
    )

    monkeypatch.setattr(
        main,
        "refresh_market_features_to_gold",
        lambda: provider_calls.append("combined_gold"),
    )

    results = asyncio.run(
        main.run_market_data_pipeline_async()
    )

    assert results["status"] == "skipped"
    assert results["run_id"] is None

    assert results["twelvedata"]["status"] == "skipped"
    assert results["marketaux"]["status"] == "skipped"
    assert results["combined_gold"]["status"] == "skipped"

    assert provider_calls == []
