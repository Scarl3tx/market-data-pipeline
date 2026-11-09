
from datetime import date, datetime, timezone

import polars as pl
import pytest

from market_data.pipelines.marketaux.silver.transform import (
    SILVER_SCHEMA,
    transform_news,
)


def sample_news():
    return {
        "retrieved_at": "2026-11-06T15:33:46.279931+00:00",
        "request": {
            "symbols": ["NVDA"],
            "page": 5,
        },
        "response": {
            "meta": {
                "found": 72203,
                "returned": 1,
                "limit": 3,
                "page": 5,
            },
            "data": [
                {
                    "uuid": "349dfc08-a2fa-40e2-870e-b86e43a62a28",
                    "title": "NVIDIA AI announcement",
                    "description": "NVIDIA announces a new AI product.",
                    "snippet": "NVIDIA announces a new product.",
                    "keywords": "",
                    "url": "https://example.com/nvidia",
                    "image_url": "https://example.com/image.jpg",
                    "language": "en",
                    "published_at": "2026-11-06T10:20:00.000000Z",
                    "source": "example.com",
                    "relevance_score": None,
                    "entities": [
                        {
                            "symbol": "NVDA",
                            "name": "NVIDIA Corporation",
                            "exchange": None,
                            "exchange_long": None,
                            "country": "us",
                            "type": "equity",
                            "industry": "Technology",
                            "match_score": 8.612744,
                            "sentiment_score": -0.296,
                            "highlights": [],
                        },
                        {
                            "symbol": "AMD",
                            "name": "Advanced Micro Devices",
                            "exchange": "NASDAQ",
                            "exchange_long": "NASDAQ Stock Market",
                            "country": "us",
                            "type": "equity",
                            "industry": "Technology",
                            "match_score": 4.5,
                            "sentiment_score": 0.25,
                            "highlights": [],
                        },
                    ],
                    "similar": [],
                }
            ],
        },
    }


def test_transforms_one_row_per_entity():
    result = transform_news(sample_news())

    assert result.height == 2
    assert result["ticker"].to_list() == ["NVDA", "AMD"]


def test_preserves_article_metadata():
    result = transform_news(sample_news())

    assert result["article_id"].to_list() == [
        "349dfc08-a2fa-40e2-870e-b86e43a62a28",
        "349dfc08-a2fa-40e2-870e-b86e43a62a28",
    ]
    assert result["title"].to_list() == [
        "NVIDIA AI announcement",
        "NVIDIA AI announcement",
    ]
    assert result["source"].to_list() == [
        "example.com",
        "example.com",
    ]


def test_extracts_entity_metadata():
    result = transform_news(sample_news())

    nvda = result.filter(pl.col("ticker") == "NVDA").row(0, named=True)

    assert nvda["company_name"] == "NVIDIA Corporation"
    assert nvda["country"] == "us"
    assert nvda["entity_type"] == "equity"
    assert nvda["industry"] == "Technology"
    assert nvda["match_score"] == pytest.approx(8.612744)
    assert nvda["sentiment_score"] == pytest.approx(-0.296)


def test_converts_timestamps_to_utc():
    result = transform_news(sample_news())

    assert result["published_at"][0] == datetime(
        2026, 11, 6, 10, 20, tzinfo=timezone.utc
    )
    assert result["retrieved_at"][0] == datetime(
        2026, 11, 6, 15, 33, 46, 279931, tzinfo=timezone.utc
    )


def test_derives_publication_date_from_published_at():
    result = transform_news(sample_news())

    assert result["publication_date"].to_list() == [
        date(2026, 11, 6),
        date(2026, 11, 6),
    ]


def test_converts_optional_null_fields():
    result = transform_news(sample_news())

    nvda = result.filter(pl.col("ticker") == "NVDA").row(0, named=True)

    assert nvda["exchange"] is None
    assert nvda["image_url"] == "https://example.com/image.jpg"


def test_handles_missing_optional_article_fields():
    raw_data = sample_news()
    article = raw_data["response"]["data"][0]

    for field in [
        "description",
        "snippet",
        "keywords",
        "image_url",
        "source",
        "language",
    ]:
        article.pop(field)

    result = transform_news(raw_data)

    assert result.height == 2
    assert result["description"].to_list() == [None, None]
    assert result["snippet"].to_list() == [None, None]
    assert result["keywords"].to_list() == [None, None]


def test_handles_articles_without_entities():
    raw_data = sample_news()
    raw_data["response"]["data"][0]["entities"] = []

    result = transform_news(raw_data)

    assert result.is_empty()
    assert result.schema == SILVER_SCHEMA


def test_skips_entities_without_symbols():
    raw_data = sample_news()
    raw_data["response"]["data"][0]["entities"].append(
        {
            "symbol": None,
            "name": "Unknown company",
        }
    )

    result = transform_news(raw_data)

    assert result.height == 2


def test_handles_empty_article_list():
    raw_data = sample_news()
    raw_data["response"]["data"] = []

    result = transform_news(raw_data)

    assert result.is_empty()
    assert result.schema == SILVER_SCHEMA


def test_converts_invalid_optional_scores_to_null():
    raw_data = sample_news()
    entity = raw_data["response"]["data"][0]["entities"][0]
    entity["match_score"] = "invalid"
    entity["sentiment_score"] = None

    result = transform_news(raw_data)

    assert result["match_score"][0] is None
    assert result["sentiment_score"][0] is None


def test_rejects_missing_retrieved_at():
    raw_data = sample_news()
    del raw_data["retrieved_at"]

    with pytest.raises(ValueError, match="retrieved_at"):
        transform_news(raw_data)


def test_rejects_missing_published_at():
    raw_data = sample_news()
    del raw_data["response"]["data"][0]["published_at"]

    with pytest.raises(ValueError, match="published_at"):
        transform_news(raw_data)


def test_rejects_timestamp_without_timezone():
    raw_data = sample_news()
    raw_data["response"]["data"][0]["published_at"] = (
        "2026-11-06T10:20:00"
    )

    with pytest.raises(ValueError, match="timezone"):
        transform_news(raw_data)


def test_rejects_article_missing_uuid():
    raw_data = sample_news()
    del raw_data["response"]["data"][0]["uuid"]

    with pytest.raises(ValueError, match="uuid"):
        transform_news(raw_data)
