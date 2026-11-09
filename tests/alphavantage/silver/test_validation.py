from datetime import date, datetime

import polars as pl
import pytest

from market_data.pipelines.alphavantage.silver.validation import (
    AlphaVantageSilverValidationError,
    validate_silver_news,
)


def valid_data():
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


def test_valid_silver_news_passes():
    validate_silver_news(valid_data())


def test_empty_silver_news_passes():
    data = valid_data().head(0)

    validate_silver_news(data)


def test_missing_column_raises_error():
    data = valid_data().drop("source")

    with pytest.raises(
        AlphaVantageSilverValidationError,
        match="Invalid Silver news columns",
    ):
        validate_silver_news(data)


def test_null_required_value_raises_error():
    data = valid_data().with_columns(
        pl.lit(None, dtype=pl.String).alias("ticker")
    )

    with pytest.raises(
        AlphaVantageSilverValidationError,
        match="ticker.*null",
    ):
        validate_silver_news(data)


def test_duplicate_article_ticker_raises_error():
    data = pl.concat([
        valid_data(),
        valid_data(),
    ])

    with pytest.raises(
        AlphaVantageSilverValidationError,
        match="Duplicate article-ticker",
    ):
        validate_silver_news(data)


def test_sentiment_score_outside_range_raises_error():
    data = valid_data().with_columns(
        pl.lit(1.5).alias("ticker_sentiment_score")
    )

    with pytest.raises(
        AlphaVantageSilverValidationError,
        match="outside",
    ):
        validate_silver_news(data)


def test_mismatched_publication_date_raises_error():
    data = valid_data().with_columns(
        pl.lit(date(2026, 11, 7)).alias("date")
    )

    with pytest.raises(
        AlphaVantageSilverValidationError,
        match="Publication date",
    ):
        validate_silver_news(data)