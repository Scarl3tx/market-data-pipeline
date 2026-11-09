
from datetime import date, datetime, timezone

import polars as pl
import pytest

from market_data.pipelines.marketaux.silver.validation import (
    EXPECTED_COLUMNS,
    MarketauxSilverValidationError,
    validate_silver_news,
)


def valid_data():
    return pl.DataFrame(
        {
            "article_id": ["abc"],
            "title": ["NVIDIA AI announcement"],
            "description": ["NVIDIA announces a new AI product."],
            "snippet": ["NVIDIA announces a new product."],
            "source": ["example.com"],
            "language": ["en"],
            "published_at": [
                datetime(2026, 11, 6, 10, 20, tzinfo=timezone.utc)
            ],
            "publication_date": [date(2026, 11, 6)],
            "retrieved_at": [
                datetime(2026, 11, 6, 15, 33, tzinfo=timezone.utc)
            ],
            "url": ["https://example.com/article"],
            "image_url": [None],
            "keywords": [""],
            "ticker": ["NVDA"],
            "company_name": ["NVIDIA Corporation"],
            "exchange": [None],
            "country": ["us"],
            "entity_type": ["equity"],
            "industry": ["Technology"],
            "match_score": [8.612744],
            "sentiment_score": [-0.296],
        }
    )


def test_valid_silver_news_passes():
    validate_silver_news(valid_data())


def test_empty_silver_news_passes():
    validate_silver_news(valid_data().head(0))


def test_non_dataframe_raises_error():
    with pytest.raises(
        MarketauxSilverValidationError,
        match="Polars DataFrame",
    ):
        validate_silver_news({"article_id": ["abc"]})


def test_missing_column_raises_error():
    data = valid_data().drop("source")

    with pytest.raises(
        MarketauxSilverValidationError,
        match="Invalid Silver news columns",
    ):
        validate_silver_news(data)


def test_null_required_value_raises_error():
    data = valid_data().with_columns(
        pl.lit(None, dtype=pl.String).alias("ticker")
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match="ticker.*null",
    ):
        validate_silver_news(data)


def test_duplicate_article_ticker_raises_error():
    data = pl.concat([valid_data(), valid_data()])

    with pytest.raises(
        MarketauxSilverValidationError,
        match="Duplicate article-ticker",
    ):
        validate_silver_news(data)


@pytest.mark.parametrize(
    ("column", "replacement"),
    [
        ("article_id", pl.lit(123)),
        ("title", pl.lit(123)),
        ("published_at", pl.lit("2026-11-06")),
        ("publication_date", pl.lit("2026-11-06")),
        ("retrieved_at", pl.lit("2026-11-06")),
    ],
)
def test_invalid_column_dtype_raises_error(column, replacement):
    data = valid_data().with_columns(
        replacement.alias(column)
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match=f"Column '{column}'.*dtype",
    ):
        validate_silver_news(data)


@pytest.mark.parametrize(
    ("column", "replacement"),
    [
        ("match_score", pl.lit("8.5")),
        ("sentiment_score", pl.lit("0.5")),
    ],
)
def test_non_float_score_dtype_raises_error(column, replacement):
    data = valid_data().with_columns(
        replacement.alias(column)
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match=f"Column '{column}'.*floating-point",
    ):
        validate_silver_news(data)


@pytest.mark.parametrize(
    "score",
    [-1.01, 1.01, float("inf"), float("-inf"), float("nan")],
)
def test_invalid_sentiment_score_raises_error(score):
    data = valid_data().with_columns(
        pl.lit(score).alias("sentiment_score")
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match="sentiment_score.*invalid",
    ):
        validate_silver_news(data)


@pytest.mark.parametrize(
    "score",
    [-0.01, float("inf"), float("-inf"), float("nan")],
)
def test_invalid_match_score_raises_error(score):
    data = valid_data().with_columns(
        pl.lit(score).alias("match_score")
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match="match_score.*invalid",
    ):
        validate_silver_news(data)


def test_match_score_can_exceed_one():
    data = valid_data().with_columns(
        pl.lit(29.728397).alias("match_score")
    )

    validate_silver_news(data)


def test_null_optional_scores_are_allowed():
    data = valid_data().with_columns(
        pl.lit(None, dtype=pl.Float64).alias("match_score"),
        pl.lit(None, dtype=pl.Float64).alias("sentiment_score"),
    )

    validate_silver_news(data)


def test_mismatched_publication_date_raises_error():
    data = valid_data().with_columns(
        pl.lit(date(2026, 11, 7)).alias("publication_date")
    )

    with pytest.raises(
        MarketauxSilverValidationError,
        match="Publication date",
    ):
        validate_silver_news(data)


def test_expected_columns_are_stable():
    assert valid_data().columns == EXPECTED_COLUMNS
