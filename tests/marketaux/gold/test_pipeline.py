from datetime import date, datetime, timezone

import polars as pl

from market_data.pipelines.marketaux.gold.pipeline import (
    run_marketaux_gold_pipeline,
)


def sample_silver():
    return pl.DataFrame({
        "article_id": ["a1", "a2"],
        "title": ["NVIDIA news 1", "NVIDIA news 2"],
        "description": ["Description 1", "Description 2"],
        "snippet": ["Snippet 1", "Snippet 2"],
        "source": ["Example", "Example"],
        "language": ["en", "en"],
        "published_at": [
            datetime(2026, 11, 6, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc),
        ],
        "publication_date": [
            date(2026, 11, 6),
            date(2026, 11, 6),
        ],
        "retrieved_at": [
            datetime(2026, 11, 6, 13, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 6, 13, 0, tzinfo=timezone.utc),
        ],
        "url": ["https://example.com/1", "https://example.com/2"],
        "image_url": [None, None],
        "keywords": ["AI", "GPU"],
        "ticker": ["NVDA", "NVDA"],
        "company_name": ["NVIDIA", "NVIDIA"],
        "exchange": [None, None],
        "country": ["us", "us"],
        "entity_type": ["equity", "equity"],
        "industry": ["Technology", "Technology"],
        "match_score": [8.0, 9.0],
        "sentiment_score": [0.5, -0.2],
    })


def test_run_marketaux_gold_pipeline_success(tmp_path, monkeypatch):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    silver_data = sample_silver()
    silver_data.write_parquet(input_path / "news_silver.pq")

    loaded = {}

    def mock_load(data):
        loaded["data"] = data
        return data.height

    monkeypatch.setattr(
        "market_data.pipelines.marketaux.gold.pipeline.load_marketaux_news_to_gold",
        mock_load,
    )

    result = run_marketaux_gold_pipeline(input_path)

    assert result == {
        "status": "success",
        "files_processed": 1,
        "rows_loaded": 1,
    }

    assert loaded["data"].height == 1
    assert loaded["data"]["article_count"][0] == 2


def test_run_marketaux_gold_pipeline_missing_folder(tmp_path):
    result = run_marketaux_gold_pipeline(tmp_path / "missing")

    assert result == {
        "status": "skipped",
        "files_processed": 0,
        "rows_loaded": 0,
    }


def test_run_marketaux_gold_pipeline_missing_file(tmp_path):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    result = run_marketaux_gold_pipeline(input_path)

    assert result == {
        "status": "skipped",
        "files_processed": 0,
        "rows_loaded": 0,
    }


def test_run_marketaux_gold_pipeline_empty_data(tmp_path, monkeypatch):
    input_path = tmp_path / "silver"
    input_path.mkdir()

    empty_data = sample_silver().head(0)
    empty_data.write_parquet(input_path / "news_silver.pq")

    def fail_if_called(data):
        raise AssertionError("Database loader should not be called.")

    monkeypatch.setattr(
        "market_data.pipelines.marketaux.gold.pipeline.load_marketaux_news_to_gold",
        fail_if_called,
    )

    result = run_marketaux_gold_pipeline(input_path)

    assert result == {
        "status": "skipped",
        "files_processed": 1,
        "rows_loaded": 0,
    }