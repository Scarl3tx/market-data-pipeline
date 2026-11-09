
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import polars as pl
import pytest

from market_data.pipelines.marketaux.silver.storage import (
    load_silver_news,
    save_silver_news,
)
from market_data.pipelines.marketaux.silver.validation import (
    MarketauxSilverValidationError,
)


def sample_data():
    return pl.DataFrame(
        {
            "article_id": ["abc"],
            "title": ["NVIDIA AI announcement"],
            "description": ["NVIDIA announces a new AI product."],
            "snippet": ["NVIDIA announces a new product."],
            "source": ["example.com"],
            "language": ["en"],
            "published_at": [
                datetime(2026, 11, 6, 10, 20, tzinfo=timezone.utc)
            ],
            "publication_date": [date(2026, 11, 6)],
            "retrieved_at": [
                datetime(2026, 11, 6, 15, 33, tzinfo=timezone.utc)
            ],
            "url": ["https://example.com/article"],
            "image_url": [None],
            "keywords": [""],
            "ticker": ["NVDA"],
            "company_name": ["NVIDIA Corporation"],
            "exchange": [None],
            "country": ["us"],
            "entity_type": ["equity"],
            "industry": ["Technology"],
            "match_score": [8.612744],
            "sentiment_score": [-0.296],
        }
    )


def test_saves_silver_news(tmp_path):
    file_path = tmp_path / "silver" / "news.pq"

    result = save_silver_news(sample_data(), file_path)

    assert result is True
    assert file_path.exists()


def test_creates_parent_directories(tmp_path):
    file_path = tmp_path / "nested" / "silver" / "news.pq"

    save_silver_news(sample_data(), file_path)

    assert file_path.exists()


def test_does_not_overwrite_existing_file(tmp_path):
    file_path = tmp_path / "news.pq"

    save_silver_news(sample_data(), file_path)

    changed_data = sample_data().with_columns(
        pl.lit("Changed title").alias("title")
    )

    result = save_silver_news(changed_data, file_path)

    assert result is False

    saved_data = pl.read_parquet(file_path)
    assert saved_data["title"][0] == "NVIDIA AI announcement"


def test_overwrites_existing_file_when_enabled(tmp_path):
    file_path = tmp_path / "news.pq"

    save_silver_news(sample_data(), file_path)

    updated_data = sample_data().with_columns(
        pl.lit("Updated title").alias("title")
    )

    result = save_silver_news(
        updated_data,
        file_path,
        overwrite=True,
    )

    assert result is True

    saved_data = pl.read_parquet(file_path)
    assert saved_data["title"][0] == "Updated title"


def test_loads_silver_news(tmp_path):
    file_path = tmp_path / "news.pq"
    original_data = sample_data()

    save_silver_news(original_data, file_path)

    loaded_data = load_silver_news(file_path)

    assert loaded_data.equals(original_data)


def test_load_missing_file_raises_error(tmp_path):
    file_path = tmp_path / "missing.pq"

    with pytest.raises(FileNotFoundError):
        load_silver_news(file_path)


def test_rejects_invalid_data_before_saving(tmp_path):
    file_path = tmp_path / "news.pq"
    invalid_data = sample_data().drop("source")

    with pytest.raises(MarketauxSilverValidationError):
        save_silver_news(invalid_data, file_path)

    assert not file_path.exists()


def test_load_rejects_invalid_silver_schema(tmp_path):
    file_path = tmp_path / "invalid.pq"

    pl.DataFrame({"invalid": ["data"]}).write_parquet(file_path)

    with pytest.raises(MarketauxSilverValidationError):
        load_silver_news(file_path)


def test_saves_empty_silver_dataframe(tmp_path):
    file_path = tmp_path / "empty.pq"
    empty_data = sample_data().head(0)

    result = save_silver_news(empty_data, file_path)

    assert result is True
    assert load_silver_news(file_path).is_empty()


def test_corrupted_parquet_raises_validation_error(tmp_path):
    file_path = tmp_path / "corrupted.pq"
    file_path.write_bytes(b"not a parquet file")

    with pytest.raises(
        MarketauxSilverValidationError,
        match="Unable to read Silver news Parquet",
    ):
        load_silver_news(file_path)


def test_temporary_file_is_removed_after_successful_save(tmp_path):
    file_path = tmp_path / "news.pq"

    save_silver_news(sample_data(), file_path)

    assert list(tmp_path.glob("*.tmp.pq")) == []
    assert list(tmp_path.glob(".*.tmp.pq")) == []


def test_temporary_file_is_removed_after_failed_write(tmp_path):
    file_path = tmp_path / "news.pq"

    invalid_data = sample_data().drop("source")

    with pytest.raises(MarketauxSilverValidationError):
        save_silver_news(invalid_data, file_path)

    assert list(tmp_path.glob("*.tmp.pq")) == []
    assert list(tmp_path.glob(".*.tmp.pq")) == []


def test_concurrent_non_overwrite_writes_do_not_replace_existing_file(
    tmp_path,
):
    file_path = tmp_path / "news.pq"

    datasets = [
        sample_data().with_columns(
            pl.lit(f"Article {index}").alias("title")
        )
        for index in range(8)
    ]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda data: save_silver_news(data, file_path),
                datasets,
            )
        )

    assert results.count(True) == 1
    assert results.count(False) == 7

    saved_data = load_silver_news(file_path)
    assert saved_data["title"][0] in {
        f"Article {index}" for index in range(8)
    }

    assert list(tmp_path.glob(".*.tmp.pq")) == []
