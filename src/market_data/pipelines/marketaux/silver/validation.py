
import polars as pl


class MarketauxSilverValidationError(Exception):
    """Raised when transformed Marketaux news data is invalid."""


EXPECTED_COLUMNS = [
    "article_id",
    "title",
    "description",
    "snippet",
    "source",
    "language",
    "published_at",
    "publication_date",
    "retrieved_at",
    "url",
    "image_url",
    "keywords",
    "ticker",
    "company_name",
    "exchange",
    "country",
    "entity_type",
    "industry",
    "match_score",
    "sentiment_score",
]

REQUIRED_COLUMNS = [
    "article_id",
    "title",
    "published_at",
    "publication_date",
    "retrieved_at",
    "url",
    "ticker",
]

STRING_COLUMNS = [
    "article_id",
    "title",
    "description",
    "snippet",
    "source",
    "language",
    "url",
    "image_url",
    "keywords",
    "ticker",
    "company_name",
    "exchange",
    "country",
    "entity_type",
    "industry",
]

DATETIME_COLUMNS = [
    "published_at",
    "retrieved_at",
]

SCORE_COLUMNS = [
    "match_score",
    "sentiment_score",
]

FLOAT_TYPES = {
    pl.Float32,
    pl.Float64,
}


def validate_silver_news(data: pl.DataFrame) -> None:
    if not isinstance(data, pl.DataFrame):
        raise MarketauxSilverValidationError(
            "Silver news data must be a Polars DataFrame."
        )

    if data.columns != EXPECTED_COLUMNS:
        raise MarketauxSilverValidationError(
            "Invalid Silver news columns. "
            f"Expected {EXPECTED_COLUMNS}, got {data.columns}."
        )

    expected_types = {
        **{column: pl.String for column in STRING_COLUMNS},
        **{column: pl.Datetime for column in DATETIME_COLUMNS},
        "publication_date": pl.Date,
        **{column: None for column in SCORE_COLUMNS},
    }

    for column, expected_type in expected_types.items():
        actual_type = data.schema[column]

        if expected_type is None:
            if actual_type not in FLOAT_TYPES:
                raise MarketauxSilverValidationError(
                    f"Column '{column}' must have a floating-point dtype, "
                    f"got {actual_type}."
                )

        elif (
            column not in REQUIRED_COLUMNS
            and actual_type == pl.Null
        ):
            # Entirely null optional columns are valid.
            continue

        elif actual_type.base_type() != expected_type:
            raise MarketauxSilverValidationError(
                f"Column '{column}' must have dtype {expected_type}, "
                f"got {actual_type}."
            )

    if data.is_empty():
        return

    for column in REQUIRED_COLUMNS:
        if data[column].null_count() > 0:
            raise MarketauxSilverValidationError(
                f"Column '{column}' contains null values."
            )

    if data.select(
        pl.struct(["article_id", "ticker"]).is_duplicated().any()
    ).item():
        raise MarketauxSilverValidationError(
            "Duplicate article-ticker combinations found."
        )

    for column in SCORE_COLUMNS:
        score = pl.col(column)

        if column == "sentiment_score":
            out_of_range = (score < -1) | (score > 1)
        else:
            out_of_range = score < 0

        invalid = data.filter(
            score.is_not_null()
            & (
                ~score.is_finite()
                | out_of_range
            )
        )

        if not invalid.is_empty():
            raise MarketauxSilverValidationError(
                f"Column '{column}' contains invalid scores."
            )

    if data.filter(
        pl.col("published_at").dt.date()
        != pl.col("publication_date")
    ).height:
        raise MarketauxSilverValidationError(
            "Publication date does not match published_at."
        )
