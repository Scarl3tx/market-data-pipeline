import polars as pl

from market_data.pipelines.marketaux.gold.transform import GOLD_SCHEMA


class MarketauxGoldValidationError(Exception):
    """Raised when Marketaux Gold data fails validation."""


EXPECTED_COLUMNS = list(GOLD_SCHEMA)


def validate_news_gold(data: pl.DataFrame) -> None:
    if not isinstance(data, pl.DataFrame):
        raise MarketauxGoldValidationError(
            "Gold data must be a Polars DataFrame."
        )

    if data.columns != EXPECTED_COLUMNS:
        raise MarketauxGoldValidationError(
            f"Unexpected columns: {data.columns}"
        )

    for column, expected_type in GOLD_SCHEMA.items():
        if data.schema[column] != expected_type:
            raise MarketauxGoldValidationError(
                f"Invalid dtype for {column}: "
                f"expected {expected_type}, got {data.schema[column]}"
            )

    if data.is_empty():
        return

    required_columns = [
        "ticker",
        "date",
        "article_count",
        "bullish_count",
        "neutral_count",
        "bearish_count",
    ]

    for column in required_columns:
        if data[column].null_count() > 0:
            raise MarketauxGoldValidationError(
                f"Column '{column}' contains null values."
            )

    if data.filter(
        pl.col("ticker").str.len_chars() == 0
    ).height:
        raise MarketauxGoldValidationError(
            "Ticker cannot be empty."
        )

    if data.select(
        pl.struct(["ticker", "date"]).is_duplicated().any()
    ).item():
        raise MarketauxGoldValidationError(
            "Duplicate ticker/date records found."
        )

    if data.filter(pl.col("article_count") == 0).height:
        raise MarketauxGoldValidationError(
            "Article count must be greater than zero."
        )

    sentiment_counts = (
        pl.col("bullish_count")
        + pl.col("neutral_count")
        + pl.col("bearish_count")
    )

    if data.filter(
        sentiment_counts > pl.col("article_count")
    ).height:
        raise MarketauxGoldValidationError(
            "Sentiment counts cannot exceed article count."
        )

    for column in [
        "avg_sentiment_score",
        "avg_match_score",
    ]:
        if data.filter(
            pl.col(column).is_not_null()
            & ~pl.col(column).is_finite()
        ).height:
            raise MarketauxGoldValidationError(
                f"Column '{column}' contains non-finite values."
            )

    if data.filter(
        pl.col("avg_sentiment_score").is_not_null()
        & (
            (pl.col("avg_sentiment_score") < -1)
            | (pl.col("avg_sentiment_score") > 1)
        )
    ).height:
        raise MarketauxGoldValidationError(
            "Average sentiment score must be between -1 and 1."
        )

    if data.filter(
        pl.col("avg_match_score").is_not_null()
        & (pl.col("avg_match_score") < 0)
    ).height:
        raise MarketauxGoldValidationError(
            "Average match score cannot be negative."
        )