from pathlib import Path

import polars as pl

from market_data.paths import ALPHAVANTAGE_SILVER_NEWS_PATH
from market_data.pipelines.alphavantage.gold.transform import (
    transform_news_gold,
)
from market_data.pipelines.alphavantage.gold.validation import (
    validate_news_gold,
)
from market_data.database.loader import load_news_to_gold
from market_data.pipelines.alphavantage.silver.storage import (
    load_silver_news,
)


def run_news_gold_pipeline(
    input_path: Path = ALPHAVANTAGE_SILVER_NEWS_PATH,
) -> dict:
    input_path = Path(input_path)

    if not input_path.exists():
        return {
            "status": "skipped",
            "files_processed": 0,
            "rows_loaded": 0,
        }

    silver_files = sorted(input_path.glob("*.parquet"))

    if not silver_files:
        return {
            "status": "skipped",
            "files_processed": 0,
            "rows_loaded": 0,
        }

    frames = [
        load_silver_news(file_path)
        for file_path in silver_files
    ]

    non_empty_frames = [
        frame for frame in frames if not frame.is_empty()
    ]

    if not non_empty_frames:
        return {
            "status": "skipped",
            "files_processed": len(silver_files),
            "rows_loaded": 0,
        }

    silver_data = pl.concat(non_empty_frames)

    # Remove duplicate article/ticker pairs across files.
    silver_data = silver_data.unique(
        subset=["article_id", "ticker"],
        keep="first",
    )

    gold_data = transform_news_gold(silver_data)

    validate_news_gold(gold_data)

    rows_loaded = load_news_to_gold(gold_data)

    return {
        "status": "success",
        "files_processed": len(silver_files),
        "rows_loaded": rows_loaded,
    }