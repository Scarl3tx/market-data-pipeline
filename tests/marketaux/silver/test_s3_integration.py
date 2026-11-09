
from datetime import datetime

import polars as pl

from market_data.pipelines.marketaux.silver import (
    pipeline as pipeline_module,
    storage as silver_storage_module,
)
from market_data.pipelines.marketaux.silver.coordinator import (
    run_marketaux_silver_coordinator,
)
from market_data.storage.local import LocalStorage


def sample_silver_data():
    return pl.DataFrame(
        {
            "article_id": ["article-001"],
            "ticker": ["NVDA"],
            "published_at": [datetime(2026, 11, 8, 10, 0)],
        }
    )


def configure_test_pipeline(monkeypatch):
    monkeypatch.setattr(
        pipeline_module,
        "transform_news",
        lambda raw_data: sample_silver_data(),
    )

    monkeypatch.setattr(
        silver_storage_module,
        "validate_silver_news",
        lambda data: None,
    )


def test_marketaux_silver_s3_prefix_workflow(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")
    configure_test_pipeline(monkeypatch)

    storage.write_json(
        "bronze/marketaux/news/2026/11/08/page_001.json",
        {"data": [{"id": "article-001"}]},
    )

    result = run_marketaux_silver_coordinator(
        storage=storage,
    )

    assert result["status"] == "success"
    assert result["files_processed"] == 1
    assert result["files_loaded"] == 1
    assert result["articles_processed"] == 1

    assert storage.exists(
        "silver/marketaux/news/files/2026/11/08/page_001.pq"
    )

    assert storage.exists(
        "silver/marketaux/news/news_silver.pq"
    )

    consolidated = storage.read_parquet(
        "silver/marketaux/news/news_silver.pq"
    )

    assert consolidated.height == 1
    assert consolidated["article_id"].to_list() == [
        "article-001"
    ]


def test_marketaux_silver_s3_workflow_is_idempotent(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")
    configure_test_pipeline(monkeypatch)

    storage.write_json(
        "bronze/marketaux/news/2026/11/08/page_001.json",
        {"data": [{"id": "article-001"}]},
    )

    first_result = run_marketaux_silver_coordinator(
        storage=storage,
    )

    second_result = run_marketaux_silver_coordinator(
        storage=storage,
    )

    assert first_result["status"] == "success"
    assert second_result["status"] == "success"

    consolidated = storage.read_parquet(
        "silver/marketaux/news/news_silver.pq"
    )

    assert consolidated.height == 1
    assert consolidated.select(
        ["article_id", "ticker"]
    ).unique().height == 1
