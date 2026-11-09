from datetime import date, datetime

import polars as pl

from market_data.pipelines.alphavantage.gold.pipeline import (
    run_news_gold_pipeline,
)
from market_data.pipelines.alphavantage.silver.storage import (
    save_silver_news,
)


def sample_silver(article_id="a1", ticker="NVDA"):
    return pl.DataFrame({
        "article_id": [article_id],
        "title": ["NVIDIA news"],
        "source": ["Example"],
        "time_published": [datetime(2026, 11, 6, 12, 0)],
        "date": [date(2026, 11, 6)],
        "url": [f"https://example.com/{article_id}"],
        "summary": ["Example summary"],
        "overall_sentiment_score": [0.5],
        "overall_sentiment_label": ["Bullish"],
        "ticker": [ticker],
        "ticker_sentiment_score": [0.8],
        "ticker_sentiment_label": ["Bullish"],
    })


def test_run_news_gold_pipeline_success(tmp_path, monkeypatch):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    save_silver_news(
        sample_silver(),
        input_path / "news_1.parquet",
    )

    loaded = {}

    def mock_load(data):
        loaded["data"] = data
        return data.height

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.gold.pipeline.load_news_to_gold",
        mock_load,
    )

    result = run_news_gold_pipeline(input_path)

    assert result == {
        "status": "success",
        "files_processed": 1,
        "rows_loaded": 1,
    }

    assert loaded["data"].height == 1


def test_run_news_gold_pipeline_combines_files(tmp_path, monkeypatch):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    save_silver_news(
        sample_silver("a1"),
        input_path / "news_1.parquet",
    )
    save_silver_news(
        sample_silver("a2"),
        input_path / "news_2.parquet",
    )

    loaded = {}

    def mock_load(data):
        loaded["data"] = data
        return data.height

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.gold.pipeline.load_news_to_gold",
        mock_load,
    )

    result = run_news_gold_pipeline(input_path)

    assert result["files_processed"] == 2
    assert result["rows_loaded"] == 1
    assert loaded["data"]["article_count"][0] == 2


def test_run_news_gold_pipeline_deduplicates_articles(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    save_silver_news(
        sample_silver("a1"),
        input_path / "news_1.parquet",
    )
    save_silver_news(
        sample_silver("a1"),
        input_path / "news_2.parquet",
    )

    loaded = {}

    def mock_load(data):
        loaded["data"] = data
        return data.height

    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.gold.pipeline.load_news_to_gold",
        mock_load,
    )

    result = run_news_gold_pipeline(input_path)

    assert result["rows_loaded"] == 1
    assert loaded["data"]["article_count"][0] == 1


def test_run_news_gold_pipeline_empty_folder(tmp_path):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    result = run_news_gold_pipeline(input_path)

    assert result == {
        "status": "skipped",
        "files_processed": 0,
        "rows_loaded": 0,
    }


def test_run_news_gold_pipeline_missing_folder(tmp_path):
    result = run_news_gold_pipeline(tmp_path / "missing")

    assert result == {
        "status": "skipped",
        "files_processed": 0,
        "rows_loaded": 0,
    }