import pytest

from market_data.pipelines.alphavantage.bronze.validation import (
    validate_news_response,
    AlphaVantageNewsValidationError,
)


def valid_response():
    return {
        "items": "1",
        "feed": [
            {
                "title": "NVIDIA announces new AI chip",
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


def test_valid_response_passes():
    validate_news_response(valid_response())


def test_empty_feed_passes():
    validate_news_response({"feed": []})


def test_non_dictionary_response_fails():
    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response([])


def test_missing_feed_fails():
    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response({"items": "1"})


def test_non_list_feed_fails():
    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response({"feed": "invalid"})


def test_non_dictionary_article_fails():
    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response({"feed": ["invalid"]})


def test_missing_article_fields_fail():
    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response({
            "feed": [{"title": "Missing fields"}]
        })


def test_invalid_ticker_sentiment_fails():
    data = valid_response()
    data["feed"][0]["ticker_sentiment"] = "invalid"

    with pytest.raises(AlphaVantageNewsValidationError):
        validate_news_response(data)