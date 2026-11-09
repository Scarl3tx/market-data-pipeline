import hashlib
from datetime import date, datetime

import polars as pl

from market_data.pipelines.alphavantage.silver.transform import (
    transform_news,
)


def sample_news():
    return {
        "feed": [
            {
                "title": "NVIDIA AI announcement",
                "url": "https://example.com/nvidia",
                "source": "Example News",
                "time_published": "20261106T120000",
                "summary": "NVIDIA announces a new AI product.",
                "overall_sentiment_score": "0.75",
                "overall_sentiment_label": "Bullish",
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.92",
                        "ticker_sentiment_label": "Bullish",
                    },
                    {
                        "ticker": "AMD",
                        "ticker_sentiment_score": "-0.30",
                        "ticker_sentiment_label": "Bearish",
                    },
                ],
            }
        ]
    }


def test_transform_news_creates_one_row_per_ticker():
    result = transform_news(sample_news())

    assert isinstance(result, pl.DataFrame)
    assert result.height == 2
    assert result["ticker"].to_list() == ["NVDA", "AMD"]


def test_transform_news_generates_deterministic_article_id():
    result = transform_news(sample_news())

    expected_id = hashlib.sha256(
        b"https://example.com/nvidia"
    ).hexdigest()

    assert result["article_id"].to_list() == [
        expected_id,
        expected_id,
    ]


def test_transform_news_converts_publication_timestamp():
    result = transform_news(sample_news())

    assert result["time_published"][0] == datetime(
        2026, 11, 6, 12, 0, 0
    )

    assert result["date"].to_list() == [
        date(2026, 11, 6),
        date(2026, 11, 6),
    ]


def test_transform_news_converts_sentiment_scores():
    result = transform_news(sample_news())

    assert result["overall_sentiment_score"].to_list() == [
        0.75,
        0.75,
    ]

    assert result["ticker_sentiment_score"].to_list() == [
        0.92,
        -0.30,
    ]


def test_transform_news_handles_missing_optional_fields():
    data = {
        "feed": [
            {
                "title": "Test article",
                "url": "https://example.com/test",
                "time_published": "20261106T120000",
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.5",
                    }
                ],
            }
        ]
    }

    result = transform_news(data)

    assert result.height == 1
    assert result["source"][0] is None
    assert result["summary"][0] is None
    assert result["overall_sentiment_score"][0] is None


def test_transform_news_handles_empty_feed():
    result = transform_news({"feed": []})

    assert result.is_empty()
    assert result.columns == [
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