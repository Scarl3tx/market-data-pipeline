
import json

import polars as pl

import market_data.pipelines.marketaux.silver.coordinator as coordinator
from market_data.pipelines.marketaux.silver.coordinator import (
    run_marketaux_silver_coordinator,
)


def sample_news(
    uuid="article-001",
    ticker="NVDA",
    title="NVIDIA AI announcement",
    published_at="2026-11-06T10:20:00.000000Z",
):
    return {
        "retrieved_at": "2026-11-06T15:33:46.279931+00:00",
        "request": {
            "symbols": [ticker],
            "page": 1,
        },
        "response": {
            "meta": {
                "found": 1,
                "returned": 1,
                "limit": 3,
                "page": 1,
            },
            "data": [
                {
                    "uuid": uuid,
                    "title": title,
                    "description": "Article description.",
                    "snippet": "Article snippet.",
                    "keywords": "",
                    "url": f"https://example.com/{uuid}",
                    "image_url": None,
                    "language": "en",
                    "published_at": published_at,
                    "source": "example.com",
                    "entities": [
                        {
                            "symbol": ticker,
                            "name": f"{ticker} Corporation",
                            "exchange": "NASDAQ",
                            "country": "us",
                            "type": "equity",
                            "industry": "Technology",
                            "match_score": 8.5,
                            "sentiment_score": 0.25,
                        }
                    ],
                    "similar": [],
                }
            ],
        },
    }


def write_bronze_file(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_consolidates_multiple_bronze_files(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "2026" / "11" / "06" / "page_001.json",
        sample_news("article-001", "NVDA"),
    )
    write_bronze_file(
        bronze_path / "2026" / "11" / "06" / "page_002.json",
        sample_news("article-002", "AMD", "AMD announcement"),
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    consolidated_file = silver_path / "news_silver.pq"

    assert result["status"] == "success"
    assert result["pipeline_status"] == "success"
    assert result["files_processed"] == 2
    assert result["files_loaded"] == 2
    assert result["files_failed"] == 0
    assert result["articles_processed"] == 2
    assert result["saved_file"] == consolidated_file
    assert consolidated_file.exists()

    data = pl.read_parquet(consolidated_file)

    assert data.height == 2
    assert set(data["ticker"].to_list()) == {"NVDA", "AMD"}


def test_deduplicates_article_ticker_combinations(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news("article-001", "NVDA", "Original title"),
    )
    write_bronze_file(
        bronze_path / "page_002.json",
        sample_news("article-001", "NVDA", "Updated title"),
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    data = pl.read_parquet(silver_path / "news_silver.pq")

    assert result["articles_processed"] == 1
    assert data.height == 1
    assert data["title"][0] == "Updated title"


def test_merges_with_existing_consolidated_silver(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news("article-002", "AMD"),
    )
    write_bronze_file(
        bronze_path / "initial.json",
        sample_news("article-001", "NVDA"),
    )

    run_marketaux_silver_coordinator(bronze_path, silver_path)

    write_bronze_file(
        bronze_path / "page_003.json",
        sample_news("article-003", "MSFT"),
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    data = pl.read_parquet(silver_path / "news_silver.pq")

    assert result["status"] == "success"
    assert data.height == 3
    assert set(data["ticker"].to_list()) == {
        "NVDA",
        "AMD",
        "MSFT",
    }


def test_repeated_run_is_idempotent(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news(),
    )

    first_result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    second_result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    data = pl.read_parquet(silver_path / "news_silver.pq")

    assert first_result["articles_processed"] == 1
    assert second_result["articles_processed"] == 1
    assert second_result["files_processed"] == 0
    assert data.height == 1


def test_skips_when_no_bronze_files_exist(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    bronze_path.mkdir()

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0
    assert result["files_loaded"] == 0
    assert result["files_failed"] == 0
    assert result["articles_processed"] == 0
    assert result["saved_file"] is None


def test_sorts_consolidated_data(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news(
            "article-later",
            "NVDA",
            published_at="2026-11-07T10:20:00Z",
        ),
    )
    write_bronze_file(
        bronze_path / "page_002.json",
        sample_news(
            "article-earlier",
            "AMD",
            published_at="2026-11-06T10:20:00Z",
        ),
    )

    run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    data = pl.read_parquet(silver_path / "news_silver.pq")

    assert data["article_id"].to_list() == [
        "article-earlier",
        "article-later",
    ]


def test_corrupt_silver_file_does_not_block_valid_files(
    tmp_path,
    monkeypatch,
):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news(),
    )

    original_pipeline = coordinator.run_marketaux_silver_pipeline

    def pipeline_with_corrupt_file(bronze, files):
        result = original_pipeline(bronze, files)

        corrupt_file = files / "corrupt.pq"
        corrupt_file.parent.mkdir(parents=True, exist_ok=True)
        corrupt_file.write_bytes(b"not a parquet file")

        return result

    monkeypatch.setattr(
        coordinator,
        "run_marketaux_silver_pipeline",
        pipeline_with_corrupt_file,
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    data = pl.read_parquet(silver_path / "news_silver.pq")

    assert result["status"] == "partial"
    assert result["files_loaded"] == 1
    assert result["files_failed"] == 1
    assert len(result["failed_files"]) == 1
    assert data.height == 1


def test_failed_consolidated_save_preserves_existing_file(
    tmp_path,
    monkeypatch,
):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news("article-001"),
    )

    first_result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    consolidated_file = first_result["saved_file"]
    original_bytes = consolidated_file.read_bytes()

    write_bronze_file(
        bronze_path / "page_002.json",
        sample_news("article-002", "AMD"),
    )

    def failed_save(*args, **kwargs):
        raise OSError("Simulated disk write failure")

    monkeypatch.setattr(
        coordinator,
        "save_silver_news",
        failed_save,
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    assert result["status"] == "partial"
    assert result["consolidation_error"] == (
        "Simulated disk write failure"
    )
    assert consolidated_file.read_bytes() == original_bytes


def test_unreadable_existing_consolidated_file_is_not_overwritten(
    tmp_path,
):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    write_bronze_file(
        bronze_path / "page_001.json",
        sample_news("article-001"),
    )

    first_result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    consolidated_file = first_result["saved_file"]

    corrupted_bytes = b"corrupted existing consolidated data"
    consolidated_file.write_bytes(corrupted_bytes)

    write_bronze_file(
        bronze_path / "page_002.json",
        sample_news("article-002", "AMD"),
    )

    result = run_marketaux_silver_coordinator(
        bronze_path,
        silver_path,
    )

    assert result["status"] == "failed"
    assert result["consolidation_error"] is not None
    assert consolidated_file.read_bytes() == corrupted_bytes
