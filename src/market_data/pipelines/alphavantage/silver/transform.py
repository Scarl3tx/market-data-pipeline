import hashlib
from datetime import datetime

import polars as pl


def transform_news(data: dict) -> pl.DataFrame:
    """
    Transform raw Alpha Vantage news into article-ticker rows.

    Each row represents one article and one ticker sentiment.
    """

    rows = []

    for article in data["feed"]:
        url = article["url"]

        article_id = hashlib.sha256(
            url.encode("utf-8")
        ).hexdigest()

        published = datetime.strptime(
            article["time_published"],
            "%Y%m%dT%H%M%S",
        )

        article_date = published.date()

        for ticker_sentiment in article["ticker_sentiment"]:
            rows.append({
                "article_id": article_id,
                "title": article["title"],
                "source": article.get("source"),
                "time_published": published,
                "date": article_date,
                "url": url,
                "summary": article.get("summary"),
                "overall_sentiment_score": _to_float(
                    article.get("overall_sentiment_score")
                ),
                "overall_sentiment_label": article.get(
                    "overall_sentiment_label"
                ),
                "ticker": ticker_sentiment["ticker"],
                "ticker_sentiment_score": _to_float(
                    ticker_sentiment.get("ticker_sentiment_score")
                ),
                "ticker_sentiment_label": ticker_sentiment.get(
                    "ticker_sentiment_label"
                ),
            })

    schema = {
        "article_id": pl.String,
        "title": pl.String,
        "source": pl.String,
        "time_published": pl.Datetime("ns"),
        "date": pl.Date,
        "url": pl.String,
        "summary": pl.String,
        "overall_sentiment_score": pl.Float64,
        "overall_sentiment_label": pl.String,
        "ticker": pl.String,
        "ticker_sentiment_score": pl.Float64,
        "ticker_sentiment_label": pl.String,
    }

    return pl.DataFrame(rows, schema=schema)


def _to_float(value) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None