import json
from pathlib import Path

import polars as pl

from market_data.paths import (
    ALPHAVANTAGE_BRONZE_NEWS_PATH,
    ALPHAVANTAGE_NEWS_SILVER_FILE,
)
from market_data.pipelines.alphavantage.silver.storage import (
    load_silver_news,
    save_silver_news,
)
from market_data.pipelines.alphavantage.silver.transform import (
    transform_news,
)


def run_news_silver_pipeline(
    bronze_path: Path = ALPHAVANTAGE_BRONZE_NEWS_PATH,
    silver_path: Path = ALPHAVANTAGE_NEWS_SILVER_FILE,
) -> dict:
    """
    Transform all Bronze news batches and merge them
    into one deduplicated Silver Parquet file.
    """

    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)

    files = sorted(bronze_path.glob("*_news.json"))

    if not files:
        return {
            "status": "skipped",
            "reason": "No Bronze news files found.",
            "articles": 0,
        }

    transformed_batches = []

    for file_path in files:
        with open(
            file_path,
            "r",
            encoding="utf-8",
        ) as file:
            raw_data = json.load(file)

        transformed = transform_news(raw_data)

        if not transformed.is_empty():
            transformed_batches.append(transformed)

    if not transformed_batches:
        return {
            "status": "skipped",
            "reason": "No ticker sentiment records found.",
            "articles": 0,
        }

    combined = pl.concat(transformed_batches)

    if silver_path.exists():
        existing = load_silver_news(silver_path)
        combined = pl.concat([existing, combined])

    combined = (
        combined
        .unique(
            subset=["article_id", "ticker"],
            keep="last",
            maintain_order=True,
        )
        .sort(["time_published", "ticker"])
    )

    save_silver_news(
        combined,
        silver_path,
        overwrite=True,
    )

    return {
        "status": "success",
        "articles": combined.height,
        "files_processed": len(files),
        "saved_file": silver_path,
    }