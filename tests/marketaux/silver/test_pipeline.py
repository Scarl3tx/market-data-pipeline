
import json

import polars as pl
import pytest

from market_data.pipelines.marketaux.silver import pipeline
from market_data.pipelines.marketaux.silver.pipeline import (
    run_marketaux_silver_pipeline,
)


def sample_news(title="NVIDIA AI announcement"):
    return {
        "retrieved_at": "2026-11-06T15:33:46.279931+00:00",
        "request": {
            "symbols": ["NVDA"],
            "page": 1,
        },
        "response": {
            "meta": {
                "found": 1,
                "returned": 1,
                "limit": 3,
                "page": 1,
            },
            "data": [
                {
                    "uuid": "349dfc08-a2fa-40e2-870e-b86e43a62a28",
                    "title": title,
                    "description": "NVIDIA announces a new AI product.",
                    "snippet": "NVIDIA announces a new product.",
                    "keywords": "",
                    "url": "https://example.com/nvidia",
                    "image_url": None,
                    "language": "en",
                    "published_at": "2026-11-06T10:20:00.000000Z",
                    "source": "example.com",
                    "entities": [
                        {
                            "symbol": "NVDA",
                            "name": "NVIDIA Corporation",
                            "exchange": None,
                            "country": "us",
                            "type": "equity",
                            "industry": "Technology",
                            "match_score": 8.612744,
                            "sentiment_score": -0.296,
                        }
                    ],
                    "similar": [],
                }
            ],
        },
    }


def write_bronze_file(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(data),
        encoding="utf-8",
    )


def test_processes_bronze_files_recursively(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    write_bronze_file(
        input_path / "2026" / "11" / "06" / "page_001.json",
        sample_news(),
    )
    write_bronze_file(
        input_path / "2026" / "11" / "07" / "page_001.json",
        sample_news("NVIDIA new GPU"),
    )

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "success"
    assert result["files_processed"] == 2
    assert result["articles_processed"] == 2
    assert result["files_skipped"] == 0
    assert result["failed"] == []

    assert (
        output_path / "2026" / "11" / "06" / "page_001.pq"
    ).exists()
    assert (
        output_path / "2026" / "11" / "07" / "page_001.pq"
    ).exists()


def test_skips_existing_silver_files(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    bronze_file = input_path / "page_001.json"
    silver_file = output_path / "page_001.pq"

    write_bronze_file(bronze_file, sample_news())

    silver_file.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"existing": ["data"]}).write_parquet(silver_file)

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0
    assert result["articles_processed"] == 0
    assert result["files_skipped"] == 1
    assert result["failed"] == []

    assert pl.read_parquet(silver_file).columns == ["existing"]


def test_skips_empty_input_directory(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    input_path.mkdir()

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0
    assert result["articles_processed"] == 0


def test_skips_missing_input_directory(tmp_path):
    result = run_marketaux_silver_pipeline(
        tmp_path / "missing",
        tmp_path / "silver",
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0


def test_skips_articles_without_entities(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    raw_data = sample_news()
    raw_data["response"]["data"][0]["entities"] = []

    write_bronze_file(
        input_path / "page_001.json",
        raw_data,
    )

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0
    assert result["articles_processed"] == 0
    assert result["files_skipped"] == 1
    assert not (output_path / "page_001.pq").exists()


def test_counts_article_entity_rows(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    raw_data = sample_news()
    raw_data["response"]["data"][0]["entities"].append(
        {
            "symbol": "AMD",
            "name": "Advanced Micro Devices",
            "exchange": "NASDAQ",
            "country": "us",
            "type": "equity",
            "industry": "Technology",
            "match_score": 4.5,
            "sentiment_score": 0.25,
        }
    )

    write_bronze_file(
        input_path / "page_001.json",
        raw_data,
    )

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "success"
    assert result["files_processed"] == 1
    assert result["articles_processed"] == 2


def test_continues_after_corrupt_json_file(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    corrupt_file = input_path / "001_corrupt.json"
    valid_file = input_path / "002_valid.json"

    corrupt_file.parent.mkdir(parents=True, exist_ok=True)
    corrupt_file.write_text("{ invalid json", encoding="utf-8")

    write_bronze_file(valid_file, sample_news())

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "partial"
    assert result["files_processed"] == 1
    assert result["articles_processed"] == 1
    assert len(result["failed"]) == 1
    assert result["failed"][0]["file"] == "001_corrupt.json"
    assert "Expecting property name" in result["failed"][0]["error"]
    assert (output_path / "002_valid.pq").exists()


def test_continues_after_transformation_failure(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    write_bronze_file(
        input_path / "001_invalid.json",
        sample_news(),
    )
    write_bronze_file(
        input_path / "002_valid.json",
        sample_news("Valid NVIDIA article"),
    )

    original_transform = pipeline.transform_news

    def failing_transform(data):
        if data["response"]["data"][0]["title"] == "NVIDIA AI announcement":
            raise ValueError("Transformation failed")

        return original_transform(data)

    monkeypatch.setattr(
        pipeline,
        "transform_news",
        failing_transform,
    )

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "partial"
    assert result["files_processed"] == 1
    assert len(result["failed"]) == 1
    assert result["failed"][0]["file"] == "001_invalid.json"
    assert result["failed"][0]["error"] == "Transformation failed"
    assert (output_path / "002_valid.pq").exists()


def test_continues_after_storage_failure(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    write_bronze_file(
        input_path / "001_storage_error.json",
        sample_news(),
    )
    write_bronze_file(
        input_path / "002_valid.json",
        sample_news("Valid NVIDIA article"),
    )

    original_save = pipeline.save_silver_news

    def failing_save(data, path):
        if path.name == "001_storage_error.pq":
            raise OSError("Disk write failed")

        return original_save(data, path)

    monkeypatch.setattr(
        pipeline,
        "save_silver_news",
        failing_save,
    )

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "partial"
    assert result["files_processed"] == 1
    assert result["articles_processed"] == 1
    assert len(result["failed"]) == 1
    assert result["failed"][0]["file"] == "001_storage_error.json"
    assert result["failed"][0]["error"] == "Disk write failed"
    assert (output_path / "002_valid.pq").exists()


def test_returns_failed_when_every_file_fails(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    corrupt_file = input_path / "corrupt.json"
    corrupt_file.parent.mkdir(parents=True, exist_ok=True)
    corrupt_file.write_text("{ invalid", encoding="utf-8")

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "failed"
    assert result["files_processed"] == 0
    assert result["articles_processed"] == 0
    assert len(result["failed"]) == 1


def test_returns_skipped_when_all_files_already_exist(tmp_path):
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"

    write_bronze_file(
        input_path / "page_001.json",
        sample_news(),
    )

    silver_file = output_path / "page_001.pq"
    silver_file.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"existing": ["data"]}).write_parquet(silver_file)

    result = run_marketaux_silver_pipeline(
        input_path,
        output_path,
    )

    assert result["status"] == "skipped"
    assert result["files_processed"] == 0
    assert result["files_skipped"] == 1
    assert result["failed"] == []


def test_rejects_input_path_that_is_a_file(tmp_path):
    input_file = tmp_path / "not_a_directory"
    input_file.write_text("test", encoding="utf-8")

    result = run_marketaux_silver_pipeline(
        input_file,
        tmp_path / "silver",
    )

    assert result["status"] == "failed"
    assert result["files_processed"] == 0
    assert len(result["failed"]) == 1
