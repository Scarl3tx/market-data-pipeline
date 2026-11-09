
from pathlib import Path

from market_data.database.loader import load_marketaux_news_to_gold
from market_data.logging_config import get_pipeline_logger
from market_data.paths import MARKETAUX_NEWS_SILVER_PATH
from market_data.pipelines.marketaux.silver.storage import load_silver_news
from market_data.pipelines.marketaux.gold.transform import transform_news_gold
from market_data.pipelines.marketaux.gold.validation import validate_news_gold
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("market_data.marketaux.gold")

S3_SILVER_KEY = "silver/marketaux/news/news_silver.pq"


def run_marketaux_gold_pipeline(
    input_path: Path = MARKETAUX_NEWS_SILVER_PATH,
    storage: StorageBackend | None = None,
) -> dict:
    logger.info("Starting Marketaux Gold pipeline.")

    if storage is not None:
        silver_file = S3_SILVER_KEY

        if not storage.exists(silver_file):
            logger.warning(
                "Marketaux consolidated Silver file does not exist: %s",
                silver_file,
            )
            return {
                "status": "skipped",
                "files_processed": 0,
                "rows_loaded": 0,
            }

        silver_data = load_silver_news(
            silver_file,
            storage=storage,
        )
    else:
        input_path = Path(input_path)

        if not input_path.exists():
            logger.warning(
                "Marketaux Silver input directory does not exist: %s",
                input_path,
            )
            return {
                "status": "skipped",
                "files_processed": 0,
                "rows_loaded": 0,
            }

        silver_file = input_path / "news_silver.pq"

        if not silver_file.exists():
            logger.warning(
                "Marketaux consolidated Silver file does not exist: %s",
                silver_file,
            )
            return {
                "status": "skipped",
                "files_processed": 0,
                "rows_loaded": 0,
            }

        silver_data = load_silver_news(silver_file)

    logger.info(
        "Loaded %s Marketaux Silver rows.",
        silver_data.height,
    )

    if silver_data.is_empty():
        logger.info("Marketaux Silver dataset is empty. Skipping Gold.")
        return {
            "status": "skipped",
            "files_processed": 1,
            "rows_loaded": 0,
        }

    gold_data = transform_news_gold(silver_data)
    validate_news_gold(gold_data)

    rows_loaded = load_marketaux_news_to_gold(gold_data)

    logger.info(
        "Marketaux Gold database load completed: %s rows upserted.",
        rows_loaded,
    )

    return {
        "status": "success",
        "files_processed": 1,
        "rows_loaded": rows_loaded,
    }
