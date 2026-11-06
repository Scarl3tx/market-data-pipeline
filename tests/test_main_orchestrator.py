
import asyncio
import pytest

import main as main_pipeline


# ---------------------------------------------------------
# Fixtures
# ---------------------------------------------------------

@pytest.fixture
def successful_pipelines(monkeypatch):
    calls = []

    def mock_av_bronze():
        calls.append("av_bronze")
        return True

    def mock_historical_bronze():
        calls.append("historical_bronze")
        return "completed"

    def mock_historical_silver():
        calls.append("historical_silver")
        return True

    def mock_commodities_silver():
        calls.append("commodities_silver")
        return True

    def mock_stocks_silver():
        calls.append("stocks_silver")
        return True

    def mock_gold():
        calls.append("gold")
        return True

    monkeypatch.setattr(
        main_pipeline,
        "run_av_bronze_coordinator",
        mock_av_bronze
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_pipeline",
        mock_historical_bronze
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_silver_pipeline",
        mock_historical_silver
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_commodities_pipeline",
        mock_commodities_silver
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_stocks_pipeline",
        mock_stocks_silver
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_gold_pipeline",
        mock_gold
    )

    return calls


# ---------------------------------------------------------
# Successful execution
# ---------------------------------------------------------

def test_main_pipeline_success(successful_pipelines):
    asyncio.run(main_pipeline.main())

    assert successful_pipelines.count("av_bronze") == 1
    assert successful_pipelines.count("historical_bronze") == 1
    assert successful_pipelines.count("historical_silver") == 1
    assert successful_pipelines.count("commodities_silver") == 1
    assert successful_pipelines.count("stocks_silver") == 1
    assert successful_pipelines.count("gold") == 1


# ---------------------------------------------------------
# Alpha Vantage Bronze failure
# ---------------------------------------------------------

def test_main_skips_av_silver_and_gold_when_bronze_fails(
    monkeypatch
):
    calls = []

    monkeypatch.setattr(
        main_pipeline,
        "run_av_bronze_coordinator",
        lambda: False
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_pipeline",
        lambda: "completed"
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_silver_pipeline",
        lambda: calls.append("historical_silver") or True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_commodities_pipeline",
        lambda: calls.append("commodities_silver") or True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_stocks_pipeline",
        lambda: calls.append("stocks_silver") or True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_gold_pipeline",
        lambda: calls.append("gold") or True
    )

    asyncio.run(main_pipeline.main())

    assert "historical_silver" in calls
    assert "commodities_silver" not in calls
    assert "stocks_silver" not in calls
    assert "gold" not in calls


# ---------------------------------------------------------
# Historical Bronze outcomes
# ---------------------------------------------------------

@pytest.mark.parametrize(
    "historical_result",
    ["quota_exhausted", "failed"]
)
def test_historical_silver_still_runs_after_historical_bronze_failure(
    monkeypatch,
    historical_result
):
    calls = []

    monkeypatch.setattr(
        main_pipeline,
        "run_av_bronze_coordinator",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_pipeline",
        lambda: historical_result
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_silver_pipeline",
        lambda: calls.append("historical_silver") or True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_commodities_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_stocks_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_gold_pipeline",
        lambda: True
    )

    asyncio.run(main_pipeline.main())

    assert "historical_silver" in calls


# ---------------------------------------------------------
# Alpha Vantage Silver failures
# ---------------------------------------------------------

@pytest.mark.parametrize(
    "failed_pipeline",
    ["commodities", "stocks"]
)
def test_gold_skipped_when_silver_pipeline_fails(
    monkeypatch,
    failed_pipeline
):
    calls = []

    monkeypatch.setattr(
        main_pipeline,
        "run_av_bronze_coordinator",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_pipeline",
        lambda: "completed"
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_silver_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_commodities_pipeline",
        lambda: failed_pipeline != "commodities"
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_stocks_pipeline",
        lambda: failed_pipeline != "stocks"
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_gold_pipeline",
        lambda: calls.append("gold") or True
    )

    asyncio.run(main_pipeline.main())

    assert "gold" not in calls


# ---------------------------------------------------------
# Gold failure
# ---------------------------------------------------------

def test_main_returns_after_gold_failure(monkeypatch):
    monkeypatch.setattr(
        main_pipeline,
        "run_av_bronze_coordinator",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_pipeline",
        lambda: "completed"
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_historical_silver_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_commodities_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_stocks_pipeline",
        lambda: True
    )
    monkeypatch.setattr(
        main_pipeline,
        "run_gold_pipeline",
        lambda: False
    )

    result = asyncio.run(main_pipeline.main())

    assert result is None
