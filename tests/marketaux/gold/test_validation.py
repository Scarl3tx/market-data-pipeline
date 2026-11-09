from datetime import date

import polars as pl
import pytest

from market_data.pipelines.marketaux.gold.transform import GOLD_SCHEMA
from market_data.pipelines.marketaux.gold.validation import (
    MarketauxGoldValidationError,
    validate_news_gold,
)


def sample_gold():
    return pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [date(2026, 11, 6)],
        "article_count": [4],
        "avg_sentiment_score": [0.2],
        "avg_match_score": [7.5],
        "bullish_count": [2],
        "neutral_count": [1],
        "bearish_count": [1],
    }, schema=GOLD_SCHEMA)


def test_validate_accepts_valid_data():
    validate_news_gold(sample_gold())


def test_validate_accepts_empty_data():
    validate_news_gold(pl.DataFrame(schema=GOLD_SCHEMA))


def test_validate_rejects_non_dataframe():
    with pytest.raises(
        MarketauxGoldValidationError,
        match="Polars DataFrame",
    ):
        validate_news_gold(None)


def test_validate_rejects_unexpected_columns():
    data = sample_gold().drop("avg_match_score")

    with pytest.raises(
        MarketauxGoldValidationError,
        match="Unexpected columns",
    ):
        validate_news_gold(data)


def test_validate_rejects_invalid_dtype():
    data = sample_gold().with_columns(
        pl.col("article_count").cast(pl.Int64)
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="Invalid dtype",
    ):
        validate_news_gold(data)


def test_validate_rejects_null_required_values():
    data = sample_gold().with_columns(
        pl.lit(None, dtype=pl.String).alias("ticker")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="contains null values",
    ):
        validate_news_gold(data)


def test_validate_allows_null_averages():
    data = sample_gold().with_columns(
        pl.lit(None, dtype=pl.Float64).alias("avg_sentiment_score"),
        pl.lit(None, dtype=pl.Float64).alias("avg_match_score"),
    )

    validate_news_gold(data)


def test_validate_rejects_duplicate_ticker_date():
    data = pl.concat([sample_gold(), sample_gold()])

    with pytest.raises(
        MarketauxGoldValidationError,
        match="Duplicate ticker/date",
    ):
        validate_news_gold(data)


def test_validate_rejects_zero_article_count():
    data = sample_gold().with_columns(
        pl.lit(0, dtype=pl.UInt32).alias("article_count")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="greater than zero",
    ):
        validate_news_gold(data)


def test_validate_rejects_excessive_sentiment_counts():
    data = sample_gold().with_columns(
        pl.lit(5, dtype=pl.UInt32).alias("bullish_count")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="cannot exceed article count",
    ):
        validate_news_gold(data)


def test_validate_rejects_out_of_range_sentiment():
    data = sample_gold().with_columns(
        pl.lit(1.5).alias("avg_sentiment_score")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="between -1 and 1",
    ):
        validate_news_gold(data)


def test_validate_rejects_negative_match_score():
    data = sample_gold().with_columns(
        pl.lit(-1.0).alias("avg_match_score")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="cannot be negative",
    ):
        validate_news_gold(data)


def test_validate_rejects_non_finite_scores():
    data = sample_gold().with_columns(
        pl.lit(float("nan")).alias("avg_match_score")
    )

    with pytest.raises(
        MarketauxGoldValidationError,
        match="non-finite",
    ):
        validate_news_gold(data)