from datetime import date, datetime

import polars as pl

from market_data.pipelines.alphavantage.gold.transform import (
    transform_news_gold,
)


def sample_silver_news():
    return pl.DataFrame(
        {
            "article_id": ["a1", "a2", "a3", "a4"],
            "title": ["News 1", "News 2", "News 3", "News 4"],
            "source": ["Source"] * 4,
            "time_published": [
                datetime(2026, 11, 6, 9, 0),
                datetime(2026, 11, 6, 10, 0),
                datetime(2026, 11, 6, 11, 0),
                datetime(2026, 11, 6, 12, 0),
            ],
            "date": [date(2026, 11, 6)] * 4,
            "url": [
                "https://example.com/1",
                "https://example.com/2",
                "https://example.com/3",
                "https://example.com/4",
            ],
            "summary": ["Summary"] * 4,
            "overall_sentiment_score": [0.5, 0.3, -0.2, 0.1],
            "overall_sentiment_label": ["Bullish"] * 4,
            "ticker": ["NVDA"] * 4,
            "ticker_sentiment_score": [0.8, 0.6, -0.4, 0.0],
            "ticker_sentiment_label": [
                "Bullish",
                "Somewhat-Bullish",
                "Bearish",
                "Neutral",
            ],
        }
    )


def test_transform_news_gold_aggregates_daily_sentiment():
    result = transform_news_gold(sample_silver_news())

    assert result.height == 1
    row = result.row(0, named=True)

    assert row["ticker"] == "NVDA"
    assert row["date"] == date(2026, 11, 6)
    assert row["article_count"] == 4
    assert row["avg_sentiment_score"] == 0.25
    assert row["bullish_count"] == 2
    assert row["neutral_count"] == 1
    assert row["bearish_count"] == 1


def test_transform_news_gold_groups_by_ticker_and_date():
    data = sample_silver_news().with_columns(
        pl.when(pl.int_range(pl.len()) == 3)
        .then(pl.lit("AMD"))
        .otherwise(pl.col("ticker"))
        .alias("ticker")
    )

    result = transform_news_gold(data)

    assert result.height == 2
    assert result["ticker"].to_list() == ["AMD", "NVDA"]


def test_transform_news_gold_handles_empty_dataframe():
    result = transform_news_gold(sample_silver_news().head(0))

    assert result.is_empty()
    assert result.columns == [
        "ticker",
        "date",
        "article_count",
        "avg_sentiment_score",
        "bullish_count",
        "neutral_count",
        "bearish_count",
    ]