import json

from alphavantage_trackers.paths import CONFIG_PATH
from alphavantage_trackers.pipelines.queue.av_queue import AlphaVantageQueue
from alphavantage_trackers.pipelines.queue.av_worker import run_av_queue_worker
from alphavantage_trackers.logging_config import get_pipeline_logger

logger = get_pipeline_logger("av_bronze_coordinator")


def run_av_bronze_coordinator() -> bool:

    logger.info("========== ALPHA VANTAGE BRONZE COORDINATOR ==========")

    # --------------------------------
    # LOAD STOCK CONFIGURATION
    # --------------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_stock_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            stock_config_data = json.load(file)

        stock_configs = stock_config_data.get("stocks", {})

    except (FileNotFoundError, json.JSONDecodeError):
        logger.exception("Failed to load stock configuration.")
        return False

    # --------------------------------
    # LOAD COMMODITY CONFIGURATION
    # --------------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_commodities_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            commodity_config_data = json.load(file)

        commodity_configs = commodity_config_data.get("commodities", {})

    except (FileNotFoundError, json.JSONDecodeError):
        logger.exception("Failed to load commodity configuration.")
        return False

    # --------------------------------
    # POPULATE SHARED QUEUE
    # --------------------------------

    queue = AlphaVantageQueue()

    queue.add_jobs(
        asset_type="stock",
        configs=stock_configs
    )

    queue.add_jobs(
        asset_type="commodities",
        configs=commodity_configs
    )

    logger.info("Stock and commodity jobs added to shared queue.")

    # --------------------------------
    # PROCESS QUEUED JOBS
    # --------------------------------

    return run_av_queue_worker(queue)