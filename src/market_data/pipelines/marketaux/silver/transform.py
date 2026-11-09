
from datetime import date, datetime, timezone

import polars as pl

from market_data.pipelines.marketaux.silver.validation import (
    EXPECTED_COLUMNS,
)


SILVER_SCHEMA = {
    "article_id": pl.String,
    "title": pl.String,
    "description": pl.String,
    "snippet": pl.String,
    "source": pl.String,
    "language": pl.String,
    "published_at": pl.Datetime("us", "UTC"),
    "publication_date": pl.Date,
    "retrieved_at": pl.Datetime("us", "UTC"),
    "url": pl.String,
    "image_url": pl.String,
    "keywords": pl.String,
    "ticker": pl.String,
    "company_name": pl.String,
    "exchange": pl.String,
    "country": pl.String,
    "entity_type": pl.String,
    "industry": pl.String,
    "match_score": pl.Float64,
    "sentiment_score": pl.Float64,
}


def _to_datetime(value, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing or invalid '{field_name}' timestamp.")

    try:
        parsed = datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            f"Invalid '{field_name}' timestamp: {value!r}"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(
            f"Timestamp '{field_name}' must include timezone information."
        )

    return parsed.astimezone(timezone.utc)


def _to_string(value):
    if value is None:
        return None

    if isinstance(value, str):
        return value

    return str(value)


def _to_float(value):
    if value is None or isinstance(value, bool):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_keywords(value):
    if value is None:
        return None

    if isinstance(value, list):
        return ", ".join(str(item) for item in value)

    return _to_string(value)


def transform_news(raw_data: dict) -> pl.DataFrame:
    if not isinstance(raw_data, dict):
        raise ValueError("Marketaux Bronze data must be a dictionary.")

    wrapper_retrieved_at = _to_datetime(
        raw_data.get("retrieved_at"),
        "retrieved_at",
    )

    response = raw_data.get("response")
    if not isinstance(response, dict):
        raise ValueError("Marketaux Bronze data has no valid response.")

    articles = response.get("data")
    if not isinstance(articles, list):
        raise ValueError("Marketaux response data must be a list.")

    rows = []

    for article_index, article in enumerate(articles):
        if not isinstance(article, dict):
            raise ValueError(
                f"Article at index {article_index} must be a dictionary."
            )

        article_id = _to_string(article.get("uuid"))
        title = _to_string(article.get("title"))
        url = _to_string(article.get("url"))

        published_at = _to_datetime(
            article.get("published_at"),
            "published_at",
        )

        if not article_id or not title or not url:
            raise ValueError(
                f"Article at index {article_index} is missing "
                "a required field (uuid, title, or url)."
            )

        entities = article.get("entities", [])

        if entities is None:
            entities = []

        if not isinstance(entities, list):
            raise ValueError(
                f"Article {article_id} has invalid entities."
            )

        for entity_index, entity in enumerate(entities):
            if not isinstance(entity, dict):
                raise ValueError(
                    f"Entity {entity_index} in article "
                    f"{article_id} must be a dictionary."
                )

            ticker = _to_string(entity.get("symbol"))

            if not ticker:
                continue

            rows.append(
                {
                    "article_id": article_id,
                    "title": title,
                    "description": _to_string(
                        article.get("description")
                    ),
                    "snippet": _to_string(article.get("snippet")),
                    "source": _to_string(article.get("source")),
                    "language": _to_string(article.get("language")),
                    "published_at": published_at,
                    "publication_date": published_at.date(),
                    "retrieved_at": wrapper_retrieved_at,
                    "url": url,
                    "image_url": _to_string(article.get("image_url")),
                    "keywords": _to_keywords(article.get("keywords")),
                    "ticker": ticker,
                    "company_name": _to_string(entity.get("name")),
                    "exchange": _to_string(entity.get("exchange")),
                    "country": _to_string(entity.get("country")),
                    "entity_type": _to_string(entity.get("type")),
                    "industry": _to_string(entity.get("industry")),
                    "match_score": _to_float(
                        entity.get("match_score")
                    ),
                    "sentiment_score": _to_float(
                        entity.get("sentiment_score")
                    ),
                }
            )

    if not rows:
        return pl.DataFrame(schema=SILVER_SCHEMA)

    return pl.DataFrame(
        rows,
        schema=SILVER_SCHEMA,
    ).select(EXPECTED_COLUMNS)
