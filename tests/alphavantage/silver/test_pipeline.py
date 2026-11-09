import json

from market_data.pipelines.alphavantage.silver.pipeline import (
    run_news_silver_pipeline,
)


def sample_news():
    return {
        "feed": [
            {
                "title": "NVIDIA announces new AI chip",
                "source": "Example News",
                "time_published": "20261106T120000",
                "url": "https://example.com/article-1",
                "summary": "NVIDIA announced a new AI chip.",
                "overall_sentiment_score": "0.5",
                "overall_sentiment_label": "Bullish",
                "ticker_sentiment": [
                    {
                        "ticker": "NVDA",
                        "ticker_sentiment_score": "0.7",
                        "ticker_sentiment_label": "Bullish",
                    }
                ],
            }
        ]
    }


def test_run_news_silver_pipeline_success(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    input_path.mkdir()

    raw_file = input_path / "news.json"
    raw_file.write_text(
        json.dumps(sample_news()),
        encoding="utf-8",
    )

    result = run_news_silver_pipeline(
        input_path=input_path,
        output_path=output_path,
    )

    assert result == {
        "status": "success",
        "files_processed": 1,
        "articles_processed": 1,
    }

    assert (output_path / "news.parquet").exists()


def test_run_news_silver_pipeline_multiple_files(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    input_path.mkdir()

    for filename in ["news_1.json", "news_2.json"]:
        (input_path / filename).write_text(
            json.dumps(sample_news()),
            encoding="utf-8",
        )

    result = run_news_silver_pipeline(
        input_path=input_path,
        output_path=output_path,
    )

    assert result["files_processed"] == 2
    assert result["articles_processed"] == 2


def test_run_news_silver_pipeline_skips_existing_output(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    input_path.mkdir()
    output_path.mkdir()

    raw_file = input_path / "news.json"
    raw_file.write_text(
        json.dumps(sample_news()),
        encoding="utf-8",
    )

    existing_file = output_path / "news.parquet"
    existing_file.write_text("existing")

    result = run_news_silver_pipeline(
        input_path=input_path,
        output_path=output_path,
    )

    assert result["files_processed"] == 0
    assert result["articles_processed"] == 0
    assert existing_file.read_text(encoding="utf-8") == "existing"


def test_run_news_silver_pipeline_empty_folder(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    input_path.mkdir()

    result = run_news_silver_pipeline(
        input_path=input_path,
        output_path=output_path,
    )

    assert result == {
        "status": "skipped",
        "files_processed": 0,
        "articles_processed": 0,
    }