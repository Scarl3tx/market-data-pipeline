from pathlib import Path

from market_data.paths import (
    ALPHAVANTAGE_BRONZE_NEWS_PATH,
    ALPHAVANTAGE_SILVER_NEWS_PATH,
)
from market_data.pipelines.alphavantage.bronze.storage import (
    load_raw_news,
)
from market_data.pipelines.alphavantage.silver.storage import (
    save_silver_news,
)
from market_data.pipelines.alphavantage.silver.transform import (
    transform_news,
)
from market_data.pipelines.alphavantage.silver.validation import (
    validate_silver_news,
)


def run_news_silver_pipeline(
    input_path: Path = ALPHAVANTAGE_BRONZE_NEWS_PATH,
    output_path: Path = ALPHAVANTAGE_SILVER_NEWS_PATH,
) -> dict:
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        return {
            "status": "skipped",
            "files_processed": 0,
            "articles_processed": 0,
        }

    raw_files = sorted(input_path.glob("*.json"))

    if not raw_files:
        return {
            "status": "skipped",
            "files_processed": 0,
            "articles_processed": 0,
        }

    files_processed = 0
    articles_processed = 0

    for raw_file in raw_files:
        silver_file = output_path / f"{raw_file.stem}.parquet"

        if silver_file.exists():
            continue

        raw_data = load_raw_news(raw_file)
        silver_data = transform_news(raw_data)

        validate_silver_news(silver_data)

        if silver_data.is_empty():
            continue

        saved = save_silver_news(silver_data, silver_file)

        if saved:
            files_processed += 1
            articles_processed += silver_data.height

    return {
        "status": "success",
        "files_processed": files_processed,
        "articles_processed": articles_processed,
    }