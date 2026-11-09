import asyncio
import threading

from market_data.pipelines import main


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

    assert results["marketaux"]["status"] == "success"

    assert results["combined_gold"]["status"] == "success"
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