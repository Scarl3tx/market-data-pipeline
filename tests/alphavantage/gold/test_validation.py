from datetime import date

import polars as pl
import pytest

from market_data.pipelines.alphavantage.gold.validation import (
    AlphaVantageGoldValidationError,
    validate_news_gold,
)


def sample_gold():
    return pl.DataFrame(
        {
            "ticker": ["NVDA"],
            "date": [date(2026, 11, 6)],
            "article_count": [4],
            "avg_sentiment_score": [0.25],
            "bullish_count": [2],
            "neutral_count": [1],
            "bearish_count": [1],
        },
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "article_count": pl.UInt32,
            "avg_sentiment_score": pl.Float64,
            "bullish_count": pl.UInt32,
            "neutral_count": pl.UInt32,
            "bearish_count": pl.UInt32,
        },
    )


def test_valid_gold_data_passes():
    validate_news_gold(sample_gold())


def test_empty_gold_data_passes():
    validate_news_gold(sample_gold().head(0))


def test_duplicate_ticker_date_raises():
    data = pl.concat([sample_gold(), sample_gold()])

    with pytest.raises(
        AlphaVantageGoldValidationError,
        match="Duplicate ticker/date",
    ):
        validate_news_gold(data)


def test_sentiment_counts_exceeding_article_count_raise():
    data = sample_gold().with_columns(
        pl.lit(5, dtype=pl.UInt32).alias("bullish_count")
    )

    with pytest.raises(
        AlphaVantageGoldValidationError,
        match="Sentiment counts",
    ):
        validate_news_gold(data)


def test_out_of_range_average_sentiment_raises():
    data = sample_gold().with_columns(
        pl.lit(1.5).alias("avg_sentiment_score")
    )

    with pytest.raises(
        AlphaVantageGoldValidationError,
        match="between -1 and 1",
    ):
        validate_news_gold(data)


def test_zero_article_count_raises():
    data = sample_gold().with_columns(
        pl.lit(0, dtype=pl.UInt32).alias("article_count")
    )

    with pytest.raises(
        AlphaVantageGoldValidationError,
        match="greater than zero",
    ):
        validate_news_gold(data)