
from market_data.pipelines.twelvedata import main as pipeline


def test_pipeline_runs_layers_in_order(monkeypatch):
    executed = []

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        lambda: executed.append("bronze") or [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: executed.append("silver") or [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: executed.append("gold") or [],
    )

    pipeline.run_twelvedata_pipeline()

    assert executed == ["bronze", "silver", "gold"]


def test_pipeline_continues_after_daily_limit(monkeypatch):
    executed = []

    def mock_bronze():
        raise pipeline.TwelveDataDailyLimitError(
            "Daily limit reached"
        )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        mock_bronze,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: executed.append("silver") or [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: executed.append("gold") or [],
    )

    result = pipeline.run_twelvedata_pipeline()

    assert executed == ["silver", "gold"]
    assert result["daily_limit_reached"] is True
    assert result["stage_errors"]["bronze"] == "Daily limit reached"


def test_pipeline_continues_after_individual_bronze_failure(monkeypatch):
    executed = []

    bronze = [
        {"symbol": "NVDA", "success": True},
        {"symbol": "AAPL", "success": False},
    ]

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        lambda: bronze,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: executed.append("silver") or [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: executed.append("gold") or [],
    )

    result = pipeline.run_twelvedata_pipeline()

    assert result["bronze"] == bronze
    assert executed == ["silver", "gold"]


def test_pipeline_returns_all_layer_results(monkeypatch):
    bronze = [{"symbol": "NVDA", "success": True}]
    silver = [{"symbol": "NVDA", "success": True}]
    gold = [{"symbol": "NVDA", "success": True}]

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        lambda: bronze,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: silver,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: gold,
    )

    result = pipeline.run_twelvedata_pipeline()

    assert result["bronze"] == bronze
    assert result["silver"] == silver
    assert result["gold"] == gold
    assert result["daily_limit_reached"] is False
    assert result["stage_errors"] == {}


def test_pipeline_continues_after_unexpected_bronze_exception(monkeypatch):
    executed = []

    def mock_bronze():
        raise RuntimeError("Unexpected API failure")

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        mock_bronze,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: executed.append("silver") or [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: executed.append("gold") or [],
    )

    result = pipeline.run_twelvedata_pipeline()

    assert executed == ["silver", "gold"]
    assert result["stage_errors"]["bronze"] == "Unexpected API failure"
    assert result["daily_limit_reached"] is False


def test_pipeline_continues_to_gold_after_silver_exception(monkeypatch):
    executed = []

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        lambda: [],
    )

    def mock_silver():
        raise RuntimeError("Silver failed")

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        mock_silver,
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        lambda: executed.append("gold") or [],
    )

    result = pipeline.run_twelvedata_pipeline()

    assert executed == ["gold"]
    assert result["stage_errors"]["silver"] == "Silver failed"


def test_pipeline_records_gold_exception(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_bronze",
        lambda: [],
    )

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_silver",
        lambda: [],
    )

    def mock_gold():
        raise RuntimeError("Gold failed")

    monkeypatch.setattr(
        pipeline,
        "run_twelvedata_gold",
        mock_gold,
    )

    result = pipeline.run_twelvedata_pipeline()

    assert result["gold"] == []
    assert result["stage_errors"]["gold"] == "Gold failed"
