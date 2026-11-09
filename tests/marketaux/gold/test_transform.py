from datetime import date
import pytest
import polars as pl

from market_data.pipelines.marketaux.gold.transform import (
    GOLD_SCHEMA,
    transform_news_gold,
)


def sample_silver():
    return pl.DataFrame({
        "ticker": ["NVDA", "NVDA", "NVDA", "NVDA"],
        "publication_date": [
            date(2026, 11, 6),
            date(2026, 11, 6),
            date(2026, 11, 6),
            date(2026, 11, 6),
        ],
        "sentiment_score": [
            0.5,
            0.1,
            -0.5,
            None,
        ],
        "match_score": [
            8.0,
            7.0,
            9.0,
            6.0,
        ],
    })


def test_transform_aggregates_news():
    result = transform_news_gold(sample_silver())

    assert result.height == 1
    assert result.columns == list(GOLD_SCHEMA)

    row = result.row(0, named=True)

    assert row["ticker"] == "NVDA"
    assert row["date"] == date(2026, 11, 6)
    assert row["article_count"] == 4

    assert row["avg_sentiment_score"] == pytest.approx(0.1 / 3)
    assert row["avg_match_score"] == 7.5

    assert row["bullish_count"] == 1
    assert row["neutral_count"] == 1
    assert row["bearish_count"] == 1


def test_transform_groups_by_ticker_and_date():
    data = pl.concat([
        sample_silver(),
        pl.DataFrame({
            "ticker": ["AAPL"],
            "publication_date": [date(2026, 11, 6)],
            "sentiment_score": [0.8],
            "match_score": [5.0],
        }),
    ])

    result = transform_news_gold(data)

    assert result.height == 2
    assert result["ticker"].to_list() == ["AAPL", "NVDA"]


def test_transform_handles_empty_dataframe():
    data = pl.DataFrame(schema={
        "ticker": pl.String,
        "publication_date": pl.Date,
        "sentiment_score": pl.Float64,
        "match_score": pl.Float64,
    })

    result = transform_news_gold(data)

    assert result.is_empty()
    assert result.schema == GOLD_SCHEMA


def test_transform_handles_missing_sentiment():
    data = pl.DataFrame({
        "ticker": ["NVDA", "NVDA"],
        "publication_date": [
            date(2026, 11, 6),
            date(2026, 11, 6),
        ],
        "sentiment_score": [None, None],
        "match_score": [8.0, 6.0],
    })

    result = transform_news_gold(data)

    row = result.row(0, named=True)

    assert row["article_count"] == 2
    assert row["avg_sentiment_score"] is None
    assert row["avg_match_score"] == 7.0
    assert row["bullish_count"] == 0
    assert row["neutral_count"] == 0
    assert row["bearish_count"] == 0