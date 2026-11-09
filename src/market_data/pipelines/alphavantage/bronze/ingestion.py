from datetime import datetime, timezone
from pathlib import Path

from market_data.api.alphavantage.client import AlphaVantageClient
from market_data.paths import (
    ALPHAVANTAGE_BRONZE_NEWS_PATH,
    ALPHAVANTAGE_NEWS_STATE_PATH,
)
from market_data.pipelines.alphavantage.bronze.config import (
    load_news_start_time,
)
from market_data.pipelines.alphavantage.bronze.state import (
    load_news_cursor,
    save_news_cursor,
)
from market_data.pipelines.alphavantage.bronze.storage import save_raw_news
from market_data.pipelines.alphavantage.bronze.validation import (
    validate_news_response,
)


def ingest_news(
    client: AlphaVantageClient,
    tickers: list[str] | None = None,
    output_path: Path = ALPHAVANTAGE_BRONZE_NEWS_PATH,
    state_path: Path = ALPHAVANTAGE_NEWS_STATE_PATH,
) -> dict:

    cursor = load_news_cursor(state_path)

    if cursor is None:
        cursor = load_news_start_time()

    if cursor is None:
        return {
            "saved_file": None,
            "articles": 0,
            "status": "skipped",
            "reason": "No enabled stocks configured.",
        }

    data = client.get_news_sentiment(
        tickers=tickers,
        time_from=cursor,
        limit=1000,
        sort="EARLIEST",
    )

    validate_news_response(data)

    articles = data["feed"]
    article_count = len(articles)

    if article_count == 0:
        return {
            "saved_file": None,
            "articles": 0,
        }

    latest_published = max(
        article["time_published"]
        for article in articles
    )

    # Validate the timestamp before saving anything.
    datetime.strptime(
        latest_published,
        "%Y%m%dT%H%M%S",
    )

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    ticker_label = "_".join(tickers) if tickers else "all"

    file_path = (
        Path(output_path)
        / f"{timestamp}_{ticker_label}_news.json"
    )

    saved = save_raw_news(data, file_path)

    if not saved:
        raise RuntimeError(
            f"News response was not saved: {file_path}"
        )

    next_cursor = save_news_cursor(
        latest_published=latest_published,
        state_path=state_path,
    )

    return {
        "saved_file": file_path,
        "articles": article_count,
        "status": "success",
        "next_time_from": next_cursor,
    }