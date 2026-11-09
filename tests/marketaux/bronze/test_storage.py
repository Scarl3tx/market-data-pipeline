
import json
from pathlib import Path

import pytest

from market_data.storage.local import LocalStorage
from market_data.pipelines.marketaux.bronze.storage import (
    MarketauxStorageError,
    save_raw_news,
)


@pytest.fixture
def sample_response() -> dict:
    return {
        "meta": {
            "found": 1,
            "returned": 1,
        },
        "data": [
            {
                "uuid": "article-123",
                "title": "Example financial news",
                "description": "Example description",
                "entities": [],
            }
        ],
    }


def test_saves_raw_response(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    assert output_path.exists()
    assert output_path.suffix == ".json"


def test_preserves_original_response(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    with output_path.open(encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data["response"] == sample_response


def test_saves_request_metadata(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["nvda", "aapl"],
            page=2,
            output_dir=tmp_path,
        )
    )

    with output_path.open(encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data["request"]["symbols"] == ["NVDA", "AAPL"]
    assert saved_data["request"]["page"] == 2
    assert saved_data["retrieved_at"]


def test_creates_date_partition_directories(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    assert output_path.parent.parent.parent.parent == tmp_path


def test_does_not_overwrite_existing_responses(tmp_path, sample_response):
    first_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    second_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {},
        {"data": None},
        {"data": "invalid"},
    ],
)
def test_rejects_invalid_response(tmp_path, response):
    with pytest.raises(MarketauxStorageError):
        save_raw_news(
            response=response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )


@pytest.mark.parametrize(
    "symbols",
    [
        None,
        [],
        [""],
        ["NVDA", ""],
        [123],
    ],
)
def test_rejects_invalid_symbols(tmp_path, sample_response, symbols):
    with pytest.raises(MarketauxStorageError):
        save_raw_news(
            response=sample_response,
            symbols=symbols,
            output_dir=tmp_path,
        )


@pytest.mark.parametrize("page", [0, -1, 1.5, "1", True])
def test_rejects_invalid_page(tmp_path, sample_response, page):
    with pytest.raises(MarketauxStorageError):
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            page=page,
            output_dir=tmp_path,
        )


def test_saves_complete_request_metadata(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["nvda"],
            page=3,
            limit=20,
            published_after="2026-11-01T00:00:00",
            published_before="2026-11-06T00:00:00",
            output_dir=tmp_path,
        )
    )

    with output_path.open(encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data["request"] == {
        "symbols": ["NVDA"],
        "page": 3,
        "limit": 20,
        "published_after": "2026-11-01T00:00:00",
        "published_before": "2026-11-06T00:00:00",
    }


@pytest.mark.parametrize("limit", [0, -1, 1.5, "20", True])
def test_rejects_invalid_limit(tmp_path, sample_response, limit):
    with pytest.raises(MarketauxStorageError):
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            limit=limit,
            output_dir=tmp_path,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("published_after", ""),
        ("published_after", 123),
        ("published_before", ""),
        ("published_before", 123),
    ],
)
def test_rejects_invalid_date_filter(
    tmp_path, sample_response, field, value
):
    with pytest.raises(MarketauxStorageError):
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
            **{field: value},
        )


def test_omits_unsupplied_optional_metadata(tmp_path, sample_response):
    output_path = Path(
        save_raw_news(
            response=sample_response,
            symbols=["NVDA"],
            output_dir=tmp_path,
        )
    )

    with output_path.open(encoding="utf-8") as file:
        saved_data = json.load(file)

    assert saved_data["request"] == {
        "symbols": ["NVDA"],
        "page": 1,
    }


def test_saves_raw_response_through_storage_backend(
    tmp_path,
    sample_response,
):
    storage = LocalStorage(tmp_path)

    key = save_raw_news(
        response=sample_response,
        symbols=["NVDA"],
        page=2,
        storage=storage,
    )

    assert isinstance(key, str)
    assert key.startswith("bronze/marketaux/news/")
    assert key.endswith(".json")
    assert storage.exists(key)

    saved_data = storage.read_json(key)

    assert saved_data["response"] == sample_response
    assert saved_data["request"]["symbols"] == ["NVDA"]
    assert saved_data["request"]["page"] == 2


def test_storage_backend_preserves_complete_request_metadata(
    tmp_path,
    sample_response,
):
    storage = LocalStorage(tmp_path)

    storage_key = save_raw_news(
        response=sample_response,
        symbols=["nvda"],
        page=3,
        limit=20,
        published_after="2026-11-01T00:00:00",
        published_before="2026-11-06T00:00:00",
        storage=storage,
    )

    assert isinstance(storage_key, str)

    saved_data = storage.read_json(storage_key)

    assert saved_data["request"] == {
        "symbols": ["NVDA"],
        "page": 3,
        "limit": 20,
        "published_after": "2026-11-01T00:00:00",
        "published_before": "2026-11-06T00:00:00",
    }


def test_storage_backend_does_not_overwrite_responses(
    tmp_path,
    sample_response,
):
    storage = LocalStorage(tmp_path)

    first_key = save_raw_news(
        response=sample_response,
        symbols=["NVDA"],
        storage=storage,
    )

    second_key = save_raw_news(
        response=sample_response,
        symbols=["NVDA"],
        storage=storage,
    )

    assert isinstance(first_key, str)
    assert isinstance(second_key, str)

    assert first_key != second_key
    assert storage.exists(first_key)
    assert storage.exists(second_key)
