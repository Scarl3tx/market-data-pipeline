import json
from pathlib import Path

from alphavantage_trackers.pipelines.av_stocks.bronze.extract_alphavantage_stocks_data import (
    run_alphavantage_stock_bronze
)

from alphavantage_trackers.pipelines.av_stocks.silver.silver_transform_alphavantage_stocks import (
    run_alphavantage_stocks_silver
)

from alphavantage_trackers.paths import (
    CONFIG_PATH,
    AV_STOCKS_PATH
)

from alphavantage_trackers.logging_config import get_pipeline_logger

logger = get_pipeline_logger("stocks")

def run_stocks_pipeline():

    logger.info("\n========== COMMODITIES PIPELINE ==========")

    # -------------------------
    # LOAD CONFIGURATION
    # -------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_stock_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            config_data = json.load(file)

        stocks_config = config_data.get("stocks", {})

        if not stocks_config:
            logger.warning("[STOCKS] No configurations found.")
            return False

    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.exception(f"[STOCKS] Configuration error: {exc}")
        return False

    # -------------------------
    # BRONZE
    # -------------------------

    logger.info("\n========== [STOCKS] Starting Bronze ==========")

    bronze_success = run_alphavantage_stock_bronze(
        configs=stocks_config
    )

    if not bronze_success:
        logger.error("[STOCKS] Bronze failed. Skipping Silver.")
        return False

    logger.info("\n========== [STOCKS] Bronze completed successfully ==========")

    # -------------------------
    # SILVER
    # -------------------------

    logger.info("\n========== [STOCKS] Starting Silver ==========")

    try:
        with open(
            CONFIG_PATH / "alphavantage_stock_schemas.json",
            "r",
            encoding="utf-8"
        ) as file:
            schema_config = json.load(file)

        if not schema_config:
            logger.error("[STOCKS] No schema configurations found.")
            return False
        
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.exception(f"[STOCKS] Configuration error: {exc}")
        return False

    try:
        with open(
            AV_STOCKS_PATH / "silver" / "silver_alphavantage_stocks_output_schema.json",
            "r",
            encoding="utf-8"
        ) as file:
            silver_output_schema = json.load(file)

        if not silver_output_schema:
            logger.error("[STOCKS] No schema configurations found.")
            return False
        
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.exception(f"[STOCKS] Configuration error: {exc}")
        return False

    silver_success = run_alphavantage_stocks_silver(
        configs=stocks_config,
        asset_schemas=schema_config,
        silver_output_schema=silver_output_schema
    )

    if not silver_success:
        logger.error("[STOCKS] Silver failed.")
        return False

    logger.info("\n========== [STOCKS] Silver completed successfully ==========")

    logger.info("\n========== STOCKS PIPELINE COMPLETE ==========\n")

    return True