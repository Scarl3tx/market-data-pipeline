import json

from market_data.pipelines.alphavantage.silver.coordinator import (
    run_news_silver_pipeline,
)
from market_data.pipelines.alphavantage.silver.storage import (
    load_silver_news,
)


def sample_news(title="NVIDIA AI announcement"):
    return {
        "feed": [
            {
                "title": title,
                "url": "https://example.com/nvidia",
                "source": "Example News",
                "time_published": "20261106T120000",
                "summary": "NVIDIA announces a new AI product.",
                "overall_sentiment_score": "0.75",
                "overall_sentiment_label": "Bullish",
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.92",
                        "ticker_sentiment_label": "Bullish",
                    },
                    {
                        "ticker": "AMD",
                        "ticker_sentiment_score": "-0.30",
                        "ticker_sentiment_label": "Bearish",
                    },
                ],
            }
        ]
    }


def write_news(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file)


def test_coordinator_skips_when_no_bronze_files(tmp_path):
    result = run_news_silver_pipeline(
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver" / "news.pq",
    )

    assert result["status"] == "skipped"
    assert result["articles"] == 0


def test_coordinator_transforms_bronze_files(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "news.pq"

    write_news(
        bronze_path / "batch1_news.json",
        sample_news(),
    )

    result = run_news_silver_pipeline(
        bronze_path=bronze_path,
        silver_path=silver_path,
    )

    assert result["status"] == "success"
    assert result["articles"] == 2
    assert result["files_processed"] == 1
    assert silver_path.exists()

    data = load_silver_news(silver_path)

    assert data.height == 2
    assert set(data["ticker"].to_list()) == {"NVDA", "AMD"}


def test_coordinator_merges_multiple_bronze_files(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "news.pq"

    write_news(
        bronze_path / "batch1_news.json",
        sample_news(),
    )

    second_news = {
        "feed": [
            {
                "title": "AMD announcement",
                "url": "https://example.com/amd",
                "time_published": "20261106T130000",
                "ticker_sentiment": [
                    {
                        "ticker": "AMD",
                        "ticker_sentiment_score": "0.50",
                        "ticker_sentiment_label": "Bullish",
                    }
                ],
            }
        ]
    }

    write_news(
        bronze_path / "batch2_news.json",
        second_news,
    )

    result = run_news_silver_pipeline(
        bronze_path=bronze_path,
        silver_path=silver_path,
    )

    assert result["status"] == "success"
    assert result["articles"] == 3
    assert result["files_processed"] == 2


def test_coordinator_deduplicates_existing_silver_data(tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "news.pq"

    write_news(
        bronze_path / "batch1_news.json",
        sample_news(),
    )

    run_news_silver_pipeline(
        bronze_path=bronze_path,
        silver_path=silver_path,
    )

    # Run again using the same Bronze files.
    result = run_news_silver_pipeline(
        bronze_path=bronze_path,
        silver_path=silver_path,
    )

    data = load_silver_news(silver_path)

    assert result["status"] == "success"
    assert result["articles"] == 2
    assert data.height == 2


def test_coordinator_skips_files_without_ticker_sentiment(tmp_path):
    bronze_path = tmp_path / "bronze"

    write_news(
        bronze_path / "batch1_news.json",
        {"feed": []},
    )

    result = run_news_silver_pipeline(
        bronze_path=bronze_path,
        silver_path=tmp_path / "silver" / "news.pq",
    )

    assert result["status"] == "skipped"
    assert result["articles"] == 0