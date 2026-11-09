from datetime import date, datetime

import polars as pl

from market_data.pipelines.alphavantage.silver.storage import (
    load_silver_news,
    save_silver_news,
)


def sample_data():
    return pl.DataFrame({
        "article_id": ["abc"],
        "title": ["NVIDIA AI announcement"],
        "source": ["Example News"],
        "time_published": [
            datetime(2026, 11, 6, 12, 0, 0)
        ],
        "date": [date(2026, 11, 6)],
        "url": ["https://example.com/nvidia"],
        "summary": ["NVIDIA announces a new AI product."],
        "overall_sentiment_score": [0.75],
        "overall_sentiment_label": ["Bullish"],
        "ticker": ["NVDA"],
        "ticker_sentiment_score": [0.92],
        "ticker_sentiment_label": ["Bullish"],
    })


def test_save_silver_news_creates_parquet_file(tmp_path):
    file_path = tmp_path / "news_silver.pq"

    result = save_silver_news(
        sample_data(),
        file_path,
    )

    assert result is True
    assert file_path.exists()


def test_save_silver_news_skips_existing_file(tmp_path):
    file_path = tmp_path / "news_silver.pq"

    save_silver_news(sample_data(), file_path)

    result = save_silver_news(
        sample_data(),
        file_path,
    )

    assert result is False


def test_load_silver_news_returns_saved_data(tmp_path):
    file_path = tmp_path / "news_silver.pq"

    original = sample_data()

    save_silver_news(original, file_path)

    loaded = load_silver_news(file_path)

    assert loaded.equals(original)


def test_save_silver_news_creates_parent_directories(tmp_path):
    file_path = (
        tmp_path
        / "silver"
        / "news"
        / "news_silver.pq"
    )

    result = save_silver_news(
        sample_data(),
        file_path,
    )

    assert result is True
    assert file_path.exists()

def test_save_silver_news_overwrites_existing_file(tmp_path):
    file_path = tmp_path / "news_silver.pq"

    original = sample_data()
    save_silver_news(original, file_path)

    updated = original.with_columns(
        pl.lit("Updated title").alias("title")
    )

    result = save_silver_news(
        updated,
        file_path,
        overwrite=True,
    )

    assert result is True
    assert load_silver_news(file_path)["title"][0] == "Updated title"