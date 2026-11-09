
import market_data.pipelines.marketaux.main as marketaux_pipeline


def test_runs_all_layers_and_returns_results(monkeypatch):
    bronze_results = {
        "status": "success",
        "successful": [{"symbol": "NVDA"}],
        "failed": [],
    }

    silver_results = {
        "status": "success",
        "files_processed": 31,
        "files_loaded": 31,
        "articles_processed": 93,
    }

    gold_results = {
        "status": "success",
        "files_processed": 1,
        "rows_loaded": 4,
    }

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_bronze_coordinator",
        lambda **kwargs: bronze_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_silver_coordinator",
        lambda: silver_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_gold_pipeline",
        lambda: gold_results,
    )

    result = marketaux_pipeline.run_marketaux_pipeline()

    assert result == {
        "bronze": bronze_results,
        "silver": silver_results,
        "gold": gold_results,
    }


def test_runs_later_layers_when_bronze_has_no_success(monkeypatch):
    bronze_results = {
        "status": "partial",
        "successful": [],
        "failed": [{"symbol": "NVDA"}],
    }

    silver_results = {"status": "success"}
    gold_results = {"status": "success"}

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_bronze_coordinator",
        lambda **kwargs: bronze_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_silver_coordinator",
        lambda: silver_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_gold_pipeline",
        lambda: gold_results,
    )

    result = marketaux_pipeline.run_marketaux_pipeline()

    assert result["bronze"] == bronze_results
    assert result["silver"] == silver_results
    assert result["gold"] == gold_results


def test_handles_partial_bronze_success(monkeypatch):
    bronze_results = {
        "status": "partial",
        "successful": [{"symbol": "NVDA"}],
        "failed": [{"symbol": "AAPL"}],
    }

    silver_results = {"status": "success"}
    gold_results = {"status": "success"}

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_bronze_coordinator",
        lambda **kwargs: bronze_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_silver_coordinator",
        lambda: silver_results,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_gold_pipeline",
        lambda: gold_results,
    )

    result = marketaux_pipeline.run_marketaux_pipeline()

    assert result["bronze"] == bronze_results
    assert result["silver"] == silver_results
    assert result["gold"] == gold_results


def test_continues_to_silver_and_gold_when_bronze_raises(monkeypatch):
    calls = []

    def failing_bronze(**kwargs):
        calls.append("bronze")
        raise RuntimeError("Bronze connection failed")

    def successful_silver():
        calls.append("silver")
        return {"status": "success"}

    def successful_gold():
        calls.append("gold")
        return {"status": "success"}

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_bronze_coordinator",
        failing_bronze,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_silver_coordinator",
        successful_silver,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_gold_pipeline",
        successful_gold,
    )

    result = marketaux_pipeline.run_marketaux_pipeline()

    assert calls == ["bronze", "silver", "gold"]
    assert result["bronze"] == {
        "status": "failed",
        "error": "Bronze connection failed",
    }
    assert result["silver"]["status"] == "success"
    assert result["gold"]["status"] == "success"


def test_continues_to_gold_when_silver_raises(monkeypatch):
    calls = []

    def successful_bronze(**kwargs):
        calls.append("bronze")
        return {"status": "success"}

    def failing_silver():
        calls.append("silver")
        raise RuntimeError("Silver consolidation failed")

    def successful_gold():
        calls.append("gold")
        return {"status": "success"}

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_bronze_coordinator",
        successful_bronze,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_silver_coordinator",
        failing_silver,
    )

    monkeypatch.setattr(
        marketaux_pipeline,
        "run_marketaux_gold_pipeline",
        successful_gold,
    )

    result = marketaux_pipeline.run_marketaux_pipeline()

    assert calls == ["bronze", "silver", "gold"]
    assert result["bronze"]["status"] == "success"
    assert result["silver"] == {
        "status": "failed",
        "error": "Silver consolidation failed",
    }
    assert result["gold"]["status"] == "success"
