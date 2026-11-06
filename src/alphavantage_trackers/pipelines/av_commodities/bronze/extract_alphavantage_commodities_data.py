import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime, timezone
import polars as pl

from alphavantage_trackers.paths import (
    CONFIG_PATH,
    AV_ENV_PATH,
    COMMODITIES_PATH
)
from alphavantage_trackers.api.alphavantage_rate_limiter import av_rate_limiter
from alphavantage_trackers.logging_config import get_pipeline_logger
from alphavantage_trackers.api.exceptions import (
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError
)
logger = get_pipeline_logger("commodities")

load_dotenv(dotenv_path=AV_ENV_PATH)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    logger.error(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    logger.info(f"✅ Success: API Key loaded securely.")

def extract_raw_commodities(asset_name: str, asset_config: dict) -> dict:
    base_url = "https://alphavantage.co/query"

    params = {
        "function": asset_config["function"],
        "apikey": API_KEY
    }

    if asset_config.get("symbol_param"):
        params[asset_config["symbol_param"]] = asset_name

    if "additional_params" in asset_config:
        params.update(asset_config["additional_params"])

    logger.info("Waiting for API request slot for %s...", asset_name)
    av_rate_limiter.acquire()

    logger.info("Sending API request for %s...", asset_name)

    try:
        response = requests.get(
            base_url,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

    except requests.RequestException:
        logger.exception("HTTP request failed for %s", asset_name)
        return {}

    except ValueError:
        logger.exception("Invalid JSON response for %s", asset_name)
        return {}

    if not isinstance(data, dict):
        logger.error("Unexpected response type for %s", asset_name)
        return {}

    if "Information" in data:
        raise AlphaVantageRateLimitError(data["Information"])

    if "Error Message" in data:
        logger.error("API error for %s: %s", asset_name, data["Error Message"])
        return {}

    if "nominal" not in data or "data" not in data:
        logger.error(
            "Unexpected commodity response structure for %s. Keys: %s",
            asset_name,
            list(data.keys())
        )
        return {}
    if not isinstance(data["data"], list):
        logger.error(
            "Invalid observation format for %s. Expected a list.",
            asset_name
        )
        return {}
    logger.info(
        "Received %d observations for %s",
        len(data["data"]) if isinstance(data["data"], list) else 0,
        asset_name
    )

    return data

def save_raw_to_disk(asset_name, data):
    if not data:
        logger.error("No data received for %s.", asset_name)
        return False

    # Create asset-specific directory
    asset_dir = COMMODITIES_PATH / asset_name
    asset_dir.mkdir(parents=True, exist_ok=True)

    # Generate hourly filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    file_path = asset_dir / f"{timestamp}_{asset_name}_raw.pq"

    # Skip if file already exists
    if file_path.exists():
        logger.info(
            "Raw data for %s already exists for this hour. Skipping.",
            asset_name
        )
        return True

    # Validate Alpha Vantage commodity response
    if "nominal" not in data or "data" not in data:
        logger.error(
            "Unexpected commodity response structure for %s. Keys received: %s",
            asset_name,
            list(data.keys())
        )
        return False

    observations = data["data"]

    if not isinstance(observations, list) or not observations:
        logger.warning("No valid observations found for %s.", asset_name)
        return False

    # Flatten observations into the existing nested Parquet structure
    records = []

    for observation in observations:
        if not isinstance(observation, dict):
            continue

        observation_date = observation.get("date")
        observation_price = observation.get("price", observation.get("value"))

        if observation_date is None or observation_price is None:
            logger.warning(
                "Skipping malformed observation for %s: %s",
                asset_name,
                observation
            )
            continue

        records.append({
            "nominal": data["nominal"],
            "data": {
                "date": observation_date,
                "price": observation_price
            }
        })

    if not records:
        logger.error("No valid records to save for %s.", asset_name)
        return False

    try:
        df = pl.DataFrame(records)

        df.write_parquet(file_path)

        logger.info(
            "Successfully saved %d observations for %s to %s",
            len(records),
            asset_name,
            file_path
        )

        return True

    except Exception:
        logger.exception("Failed to save raw commodity data for %s.", asset_name)
        return False
    
def run_alphavantage_commodities_bronze(configs: dict) -> bool:
    logger.info("Initializing commodities ingestion cycle...")

    success = True
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    for commodities_name, commodities_config in configs.items():

        expected_file = (
            COMMODITIES_PATH
            / commodities_name
            / f"{timestamp}_{commodities_name}_raw.pq"
        )

        # Skip existing hourly extraction
        if expected_file.exists():
            logger.info(
                "[API SKIPPED] Data for %s already pulled this hour.",
                commodities_name
            )
            continue

        try:
            raw_payload = extract_raw_commodities(
                commodities_name,
                commodities_config
            )

            if not raw_payload:
                success = False
                continue

            if not save_raw_to_disk(commodities_name, raw_payload):
                success = False

        except AlphaVantageQuotaExceededError as e:
            logger.warning(
                "Local API quota exhausted while processing %s. "
                "Next request available in %.0f seconds.",
                commodities_name,
                e.retry_after
            )
            success = False
            break

        except AlphaVantageRateLimitError:
            logger.exception(
                "Alpha Vantage rate limit reached while processing %s.",
                commodities_name
            )
            success = False
            break

        except Exception:
            logger.exception(
                "Unexpected error while processing %s.",
                commodities_name
            )
            success = False

    if success:
        logger.info("Commodities extraction completed successfully.")
    else:
        logger.warning("Commodities extraction completed with failures.")

    return success


if __name__ == "__main__":
    print("DEBUG 1: Main block reached")
    commodities_config_path = CONFIG_PATH / "alphavantage_commodities_config.json"

    try:
        with open(commodities_config_path, "r", encoding="utf-8") as file:
            commodities_config_data = json.load(file)
        print("DEBUG 2: Configuration loaded")
        local_configs = commodities_config_data.get("commodities", {})
        print(f"DEBUG 3: Configured assets: {list(local_configs.keys())}")

        if not local_configs:
            print("❌ No commodities configurations found.")
        else:
            run_alphavantage_commodities_bronze(configs=local_configs)

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {commodities_config_path}")
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON configuration: {e}")