import polars as pl


class AlphaVantageGoldValidationError(Exception):
    pass


EXPECTED_COLUMNS = [
    "ticker",
    "date",
    "article_count",
    "avg_sentiment_score",
    "bullish_count",
    "neutral_count",
    "bearish_count",
]

EXPECTED_SCHEMA = {
    "ticker": pl.String,
    "date": pl.Date,
    "article_count": pl.UInt32,
    "avg_sentiment_score": pl.Float64,
    "bullish_count": pl.UInt32,
    "neutral_count": pl.UInt32,
    "bearish_count": pl.UInt32,
}


def validate_news_gold(data: pl.DataFrame) -> None:
    if not isinstance(data, pl.DataFrame):
        raise AlphaVantageGoldValidationError(
            "Gold data must be a Polars DataFrame."
        )

    if data.columns != EXPECTED_COLUMNS:
        raise AlphaVantageGoldValidationError(
            f"Unexpected columns: {data.columns}"
        )

    for column, expected_type in EXPECTED_SCHEMA.items():
        if data.schema[column] != expected_type:
            raise AlphaVantageGoldValidationError(
                f"Invalid dtype for {column}: "
                f"expected {expected_type}, got {data.schema[column]}"
            )

    if data.is_empty():
        return

    for column in EXPECTED_COLUMNS:
        if data[column].null_count() > 0:
            raise AlphaVantageGoldValidationError(
                f"Column '{column}' contains null values."
            )

    if data.select(
        pl.struct(["ticker", "date"]).is_duplicated().any()
    ).item():
        raise AlphaVantageGoldValidationError(
            "Duplicate ticker/date records found."
        )

    if data.filter(pl.col("article_count") == 0).height:
        raise AlphaVantageGoldValidationError(
            "Article count must be greater than zero."
        )

    sentiment_counts = (
        pl.col("bullish_count")
        + pl.col("neutral_count")
        + pl.col("bearish_count")
    )

    if data.filter(sentiment_counts > pl.col("article_count")).height:
        raise AlphaVantageGoldValidationError(
            "Sentiment counts cannot exceed article count."
        )

    if data.filter(
        (pl.col("avg_sentiment_score") < -1)
        | (pl.col("avg_sentiment_score") > 1)
    ).height:
        raise AlphaVantageGoldValidationError(
            "Average sentiment score must be between -1 and 1."
        )