import json

from market_data.pipelines.alphavantage.bronze.storage import (
    save_raw_news,
    load_raw_news,
)


def sample_news():
    return {
        "items": "1",
        "feed": [
            {
                "title": "NVIDIA announces new AI chip",
                "time_published": "20261106T120000",
                "overall_sentiment_score": 0.85,
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.92",
                    }
                ],
            }
        ],
    }


def test_save_raw_news_creates_file(tmp_path):
    file_path = tmp_path / "news" / "NVDA.json"

    result = save_raw_news(sample_news(), file_path)

    assert result is True
    assert file_path.exists()


def test_save_raw_news_preserves_original_data(tmp_path):
    file_path = tmp_path / "NVDA.json"
    data = sample_news()

    save_raw_news(data, file_path)

    with open(file_path, "r", encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data == data


def test_save_raw_news_creates_parent_directories(tmp_path):
    file_path = tmp_path / "nested" / "news" / "NVDA.json"

    save_raw_news(sample_news(), file_path)

    assert file_path.exists()
    assert file_path.parent.exists()


def test_save_raw_news_skips_existing_file(tmp_path):
    file_path = tmp_path / "NVDA.json"
    original_data = sample_news()

    save_raw_news(original_data, file_path)

    updated_data = {"feed": []}
    result = save_raw_news(updated_data, file_path)

    assert result is False

    with open(file_path, "r", encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data == original_data


def test_load_raw_news(tmp_path):
    file_path = tmp_path / "NVDA.json"
    data = sample_news()

    save_raw_news(data, file_path)

    loaded_data = load_raw_news(file_path)

    assert loaded_data == data