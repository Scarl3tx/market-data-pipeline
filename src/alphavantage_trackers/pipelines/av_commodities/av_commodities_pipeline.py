import json
from pathlib import Path

from alphavantage_trackers.pipelines.av_commodities.bronze.extract_alphavantage_commodities_data import (
    run_alphavantage_commodities_bronze
)

from alphavantage_trackers.pipelines.av_commodities.silver.silver_transform_alphavantage_commodities import (
    run_alphavantage_commodities_silver
)

from alphavantage_trackers.paths import (
    CONFIG_PATH,
    AV_COMMODITIES_PATH,
    AV_STOCKS_PATH
)
from alphavantage_trackers.logging_config import get_pipeline_logger
logger = get_pipeline_logger("commodities")

def run_commodities_pipeline():

    logger.info("\n========== COMMODITIES PIPELINE ==========")

    # -------------------------
    # LOAD CONFIGURATION
    # -------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_commodities_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            config_data = json.load(file)

        commodities_config = config_data.get("commodities", {})

        if not commodities_config:
            logger.info("[COMMODITIES] No configurations found.")
            return False

    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.info(f"[COMMODITIES] Configuration error: {exc}")
        return False

    # -------------------------
    # BRONZE
    # -------------------------

    logger.info("\n========== [COMMODITIES] Starting Bronze ==========")

    bronze_success = run_alphavantage_commodities_bronze(
        configs=commodities_config
    )

    if not bronze_success:
        logger.info("[COMMODITIES] Bronze failed. Skipping Silver.")
        return False

    logger.info("\n========== [COMMODITIES] Bronze completed successfully ==========")

    # -------------------------
    # SILVER
    # -------------------------

    logger.info("\n========== [COMMODITIES] Starting Silver ==========")

    try:
        with open(
            CONFIG_PATH / "alphavantage_commodities_schemas.json",
            "r",
            encoding="utf-8"
        ) as file:
            schema_config = json.load(file)
        if not schema_config:
            logger.info("[COMMODITIES] No schema configurations found.")
            return False
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.info(f"[COMMODITIES] Configuration error: {exc}")
        return False

    try:
        with open(
            AV_COMMODITIES_PATH / "silver" / "silver_alphavantage_commodities_output_schema.json",
            "r",
            encoding="utf-8"
        ) as file:
            silver_commodities_output_schema = json.load(file)
        if not silver_commodities_output_schema:
            logger.info("[COMMODITIES] No schema configurations found.")
            return False
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.info(f"[COMMODITIES] Configuration error: {exc}")
        return False

    silver_success = run_alphavantage_commodities_silver(
        configs=commodities_config,
        asset_schemas=schema_config,
        silver_output_schema=silver_commodities_output_schema
    )

    if not silver_success:
        logger.info("[COMMODITIES] Silver failed.")
        return False

    logger.info("\n========== [COMMODITIES] Silver completed successfully ==========")

    logger.info("\n========== COMMODITIES PIPELINE COMPLETE ==========\n")

    return True