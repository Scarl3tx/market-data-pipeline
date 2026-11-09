import market_data.pipelines.main as pipeline_module
import pytest


@pytest.fixture(autouse=True)
def disable_run_history(monkeypatch):
    monkeypatch.setattr(
        pipeline_module,
        "save_pipeline_run",
        lambda results: results["run_id"],
    )

def test_runs_both_provider_pipelines(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline_module,
        "run_twelvedata_pipeline",
        lambda: calls.append("twelvedata") or {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_marketaux_pipeline",
        lambda: calls.append("marketaux") or {
            "status": "success",
        },
    )

    monkeypatch.setattr(
        pipeline_module,
        "refresh_market_features_to_gold",
        lambda: 100,
    )

    result = pipeline_module.run_market_data_pipeline()

    assert set(calls) == {"twelvedata", "marketaux"}
    assert result["twelvedata"]["status"] == "success"
    assert result["marketaux"]["status"] == "success"
    assert result["combined_gold"]["status"] == "success"


def test_alpha_vantage_news_is_not_executed(monkeypatch):
    calls = []

    def unexpected_call():
        calls.append("alphavantage")
        raise AssertionError(
            "Alpha Vantage news should be disabled"
        )

    monkeypatch.setattr(
        pipeline_module,
        "run_news_bronze_pipeline",
        unexpected_call,
        raising=False,
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_news_silver_pipeline",
        unexpected_call,
        raising=False,
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_news_gold_pipeline",
        unexpected_call,
        raising=False,
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_twelvedata_pipeline",
        lambda: {"status": "success"},
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_marketaux_pipeline",
        lambda: {"status": "success"},
    )

    monkeypatch.setattr(
        pipeline_module,
        "refresh_market_features_to_gold",
        lambda: 100,
    )

    pipeline_module.run_market_data_pipeline()

    assert calls == []


def test_twelvedata_failure_is_captured(monkeypatch):
    def fail_twelvedata():
        raise RuntimeError("Twelve Data failure")

    monkeypatch.setattr(
        pipeline_module,
        "run_twelvedata_pipeline",
        fail_twelvedata,
    )

    monkeypatch.setattr(
        pipeline_module,
        "run_marketaux_pipeline",
        lambda: {"status": "success"},
    )

    monkeypatch.setattr(
        pipeline_module,
        "refresh_market_features_to_gold",
        lambda: 100,
    )

    result = pipeline_module.run_market_data_pipeline()

    assert result["twelvedata"]["status"] == "failed"
    assert "Twelve Data failure" in result["twelvedata"]["error"]
    assert result["marketaux"]["status"] == "success"
    assert result["combined_gold"]["status"] == "success"