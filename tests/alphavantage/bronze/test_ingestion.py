import json
from unittest.mock import Mock

import pytest

from market_data.pipelines.alphavantage.bronze.ingestion import (
    ingest_news,
)
from market_data.pipelines.alphavantage.bronze.state import (
    load_news_cursor,
    save_news_cursor,
)
from market_data.pipelines.alphavantage.bronze.validation import (
    AlphaVantageNewsValidationError,
)


@pytest.fixture
def configure_news_start(monkeypatch):
    monkeypatch.setattr(
        "market_data.pipelines.alphavantage.bronze.ingestion.load_news_start_time",
        lambda: "20000101T0000",
    )


def sample_news():
    return {
        "items": "1",
        "feed": [
            {
                "title": "NVIDIA announces new AI chip",
                "url": "https://example.com/nvidia",
                "time_published": "20261106T120000",
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.92",
                    }
                ],
            }
        ],
    }


def test_ingest_news_fetches_and_saves_response(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = sample_news()

    state_path = tmp_path / "news_state.json"
    output_path = tmp_path / "news"

    result = ingest_news(
        client=client,
        tickers=["NVDA"],
        output_path=output_path,
        state_path=state_path,
    )

    assert result["saved_file"] is not None
    assert result["saved_file"].exists()
    assert result["articles"] == 1
    assert result["status"] == "success"
    assert result["next_time_from"] == "20261106T1201"

    client.get_news_sentiment.assert_called_once_with(
        tickers=["NVDA"],
        time_from="20000101T0000",
        limit=1000,
        sort="EARLIEST",
    )


def test_ingest_news_returns_zero_when_feed_is_empty(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = {"feed": []}

    output_path = tmp_path / "news"
    state_path = tmp_path / "news_state.json"

    result = ingest_news(
        client=client,
        output_path=output_path,
        state_path=state_path,
    )

    assert result == {
        "saved_file": None,
        "articles": 0,
    }

    assert not output_path.exists()
    assert not state_path.exists()


def test_ingest_news_passes_no_tickers_when_unspecified(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = sample_news()

    ingest_news(
        client=client,
        output_path=tmp_path / "news",
        state_path=tmp_path / "news_state.json",
    )

    client.get_news_sentiment.assert_called_once_with(
        tickers=None,
        time_from="20000101T0000",
        limit=1000,
        sort="EARLIEST",
    )


def test_ingest_news_preserves_raw_response(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    data = sample_news()
    client.get_news_sentiment.return_value = data

    result = ingest_news(
        client=client,
        output_path=tmp_path / "news",
        state_path=tmp_path / "news_state.json",
    )

    with open(
        result["saved_file"],
        "r",
        encoding="utf-8",
    ) as file:
        saved_data = json.load(file)

    assert saved_data == data


def test_ingest_news_rejects_invalid_response(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = {
        "invalid": "response"
    }

    output_path = tmp_path / "news"

    with pytest.raises(AlphaVantageNewsValidationError):
        ingest_news(
            client=client,
            output_path=output_path,
            state_path=tmp_path / "news_state.json",
        )

    assert not output_path.exists()


def test_ingest_news_rejects_invalid_article(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = {
        "feed": [
            {
                "title": "Incomplete article"
            }
        ]
    }

    output_path = tmp_path / "news"

    with pytest.raises(AlphaVantageNewsValidationError):
        ingest_news(
            client=client,
            output_path=output_path,
            state_path=tmp_path / "news_state.json",
        )

    assert not output_path.exists()


def test_ingestion_uses_initial_start_date(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = sample_news()

    result = ingest_news(
        client=client,
        tickers=["NVDA"],
        output_path=tmp_path / "news",
        state_path=tmp_path / "news_state.json",
    )

    client.get_news_sentiment.assert_called_once_with(
        tickers=["NVDA"],
        time_from="20000101T0000",
        limit=1000,
        sort="EARLIEST",
    )

    assert result["articles"] == 1
    assert result["status"] == "success"


def test_ingestion_resumes_from_saved_cursor(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = {
        "feed": [
            {
                "title": "NVIDIA news",
                "url": "https://example.com/nvidia",
                "time_published": "20260102T120000",
                "ticker_sentiment": [
                    {"ticker": "NVDA"}
                ],
            }
        ]
    }

    state_path = tmp_path / "news_state.json"

    save_news_cursor(
        "20260101T120000",
        state_path,
    )

    ingest_news(
        client=client,
        tickers=["NVDA"],
        output_path=tmp_path / "news",
        state_path=state_path,
    )

    client.get_news_sentiment.assert_called_once_with(
        tickers=["NVDA"],
        time_from="20260101T1201",
        limit=1000,
        sort="EARLIEST",
    )


def test_ingestion_does_not_advance_cursor_when_feed_empty(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.return_value = {"feed": []}

    state_path = tmp_path / "news_state.json"

    save_news_cursor(
        "20260101T120000",
        state_path,
    )

    result = ingest_news(
        client=client,
        tickers=["NVDA"],
        output_path=tmp_path / "news",
        state_path=state_path,
    )

    assert result["articles"] == 0
    assert load_news_cursor(state_path) == "20260101T1201"


def test_ingestion_does_not_advance_cursor_when_api_fails(
    tmp_path,
    configure_news_start,
):
    client = Mock()
    client.get_news_sentiment.side_effect = RuntimeError("API failed")

    state_path = tmp_path / "news_state.json"

    save_news_cursor(
        "20260101T120000",
        state_path,
    )

    with pytest.raises(RuntimeError, match="API failed"):
        ingest_news(
            client=client,
            tickers=["NVDA"],
            output_path=tmp_path / "news",
            state_path=state_path,
        )

    assert load_news_cursor(state_path) == "20260101T1201"