
from unittest.mock import Mock, patch

from market_data.pipelines.alphavantage.bronze.coordinator import (
    run_news_bronze_pipeline,
)


@patch(
    "market_data.pipelines.alphavantage.bronze.coordinator.load_news_tickers"
)
def test_coordinator_runs_pipeline(mock_tickers):
    mock_tickers.return_value = ["NVDA", "AAPL"]
    client = Mock()

    ingestion_result = {
        "saved_file": None,
        "articles": 0,
    }

    with patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.ingest_news",
        return_value=ingestion_result,
    ) as mock_ingest:

        result = run_news_bronze_pipeline(client=client)

    mock_ingest.assert_called_once_with(
        client=client,
        tickers=["NVDA", "AAPL"],
    )

    assert result["status"] == "success"
    assert result["saved_file"] is None
    assert result["articles"] == 0


@patch(
    "market_data.pipelines.alphavantage.bronze.coordinator.load_news_tickers"
)
def test_coordinator_creates_default_client(mock_tickers):
    mock_tickers.return_value = ["NVDA"]

    ingestion_result = {
        "saved_file": None,
        "articles": 0,
    }

    with patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.AlphaVantageClient"
    ) as mock_client_class, patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.ingest_news",
        return_value=ingestion_result,
    ) as mock_ingest:

        run_news_bronze_pipeline()

    mock_client_class.assert_called_once_with()

    mock_ingest.assert_called_once_with(
        client=mock_client_class.return_value,
        tickers=["NVDA"],
    )


@patch(
    "market_data.pipelines.alphavantage.bronze.coordinator.load_news_tickers"
)
def test_coordinator_returns_saved_file_and_article_count(mock_tickers, tmp_path):
    mock_tickers.return_value = ["NVDA"]
    client = Mock()
    saved_file = tmp_path / "news.json"

    ingestion_result = {
        "saved_file": saved_file,
        "articles": 5,
    }

    with patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.ingest_news",
        return_value=ingestion_result,
    ):
        result = run_news_bronze_pipeline(client=client)

    assert result["status"] == "success"
    assert result["saved_file"] == saved_file
    assert result["articles"] == 5


@patch(
    "market_data.pipelines.alphavantage.bronze.coordinator.load_news_tickers",
    return_value=[],
)
def test_coordinator_skips_when_no_tickers_enabled(mock_tickers):
    with patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.AlphaVantageClient"
    ) as mock_client_class, patch(
        "market_data.pipelines.alphavantage.bronze.coordinator.ingest_news"
    ) as mock_ingest:

        result = run_news_bronze_pipeline()

    assert result["status"] == "skipped"
    assert result["saved_file"] is None
    assert result["articles"] == 0

    mock_client_class.assert_not_called()
    mock_ingest.assert_not_called()
