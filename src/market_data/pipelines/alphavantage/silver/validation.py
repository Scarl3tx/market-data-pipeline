import polars as pl


class AlphaVantageSilverValidationError(Exception):
    """Raised when transformed Alpha Vantage news data is invalid."""


EXPECTED_COLUMNS = [
    "article_id",
    "title",
    "source",
    "time_published",
    "date",
    "url",
    "summary",
    "overall_sentiment_score",
    "overall_sentiment_label",
    "ticker",
    "ticker_sentiment_score",
    "ticker_sentiment_label",
]

REQUIRED_COLUMNS = [
    "article_id",
    "title",
    "time_published",
    "date",
    "url",
    "ticker",
]


def validate_silver_news(data: pl.DataFrame) -> None:
    if not isinstance(data, pl.DataFrame):
        raise AlphaVantageSilverValidationError(
            "Silver news data must be a Polars DataFrame."
        )

    if data.columns != EXPECTED_COLUMNS:
        raise AlphaVantageSilverValidationError(
            "Invalid Silver news columns. "
            f"Expected {EXPECTED_COLUMNS}, got {data.columns}."
        )

    if data.is_empty():
        return

    for column in REQUIRED_COLUMNS:
        if data[column].null_count() > 0:
            raise AlphaVantageSilverValidationError(
                f"Column '{column}' contains null values."
            )

    if data.select(
        pl.struct(["article_id", "ticker"]).is_duplicated().any()
    ).item():
        raise AlphaVantageSilverValidationError(
            "Duplicate article-ticker combinations found."
        )

    for column in [
        "overall_sentiment_score",
        "ticker_sentiment_score",
    ]:
        invalid = data.filter(
            data[column].is_not_null()
            & (
                (data[column] < -1)
                | (data[column] > 1)
            )
        )

        if not invalid.is_empty():
            raise AlphaVantageSilverValidationError(
                f"Column '{column}' contains scores outside [-1, 1]."
            )

    if data.filter(
        data["time_published"].dt.date() != data["date"]
    ).height:
        raise AlphaVantageSilverValidationError(
            "Publication date does not match time_published."
        )