from market_data.api.alphavantage.client import AlphaVantageClient
from market_data.pipelines.alphavantage.bronze.config import (
    load_news_tickers,
)
from market_data.pipelines.alphavantage.bronze.ingestion import ingest_news


def run_news_bronze_pipeline(
    client: AlphaVantageClient | None = None,
) -> dict:

    tickers = load_news_tickers()

    if not tickers:
        return {
            "status": "skipped",
            "reason": "No enabled stocks configured.",
            "saved_file": None,
            "articles": 0,
        }

    if client is None:
        client = AlphaVantageClient()

    result = ingest_news(
        client=client,
        tickers=tickers,
    )

    return {
    "status": "success",
    "saved_file": result["saved_file"],
    "articles": result["articles"],
    **(
        {"next_time_from": result["next_time_from"]}
        if "next_time_from" in result
        else {}
    ),
}