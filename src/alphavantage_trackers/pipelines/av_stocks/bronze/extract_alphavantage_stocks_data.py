import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime, timezone
import polars as pl

from alphavantage_trackers.paths import (
    CONFIG_PATH,
    AV_ENV_PATH,
    STOCKS_PATH
)
from alphavantage_trackers.logging_config import get_pipeline_logger
logger = get_pipeline_logger("stocks")

from alphavantage_trackers.api.alphavantage_rate_limiter import av_rate_limiter
from alphavantage_trackers.api.exceptions import (
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError
)

load_dotenv(dotenv_path=AV_ENV_PATH)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    logger.error(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    logger.info(f"✅ Success: API Key loaded securely.")

def extract_raw_stock(asset_name: str, asset_config: dict) -> dict:
    base_url = "https://alphavantage.co/query"
    params = {
        "function": asset_config["function"],
        "apikey": API_KEY
    }
    # 2. Dynamically inject the symbol parameter ONLY if the URL template requires it
    if asset_config.get("symbol_param"):
        params[asset_config["symbol_param"]] = asset_name

    # 3. Dynamically inject any custom endpoint parameters (like interval or outputsize)
    if "additional_params" in asset_config:
        params.update(asset_config["additional_params"])

    logger.info("Waiting for API request slot for %s...", asset_name)
    av_rate_limiter.acquire()
    logger.info("Sending API request for %s...", asset_name)

    try:
        response = requests.get(base_url, params=params,timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        logger.exception(f"HTTP request failed for {asset_name}")
        return {}

    except ValueError:
        logger.exception(f"Invalid JSON response for {asset_name}")
        return {}

    # 4. Data Quality Firewall: Check for the rate limit warning you received
    if not isinstance(data, dict):
        logger.error("Unexpected API response format for %s.", asset_name)
        return {}

    if "Information" in data:
        raise AlphaVantageRateLimitError(data["Information"])

    if "Error Message" in data:
        logger.error(f"❌ API Error for {asset_name}: {data['Error Message']}")
        return {}
    return data

def save_raw_to_disk(asset_name: str, data: dict) -> bool:
    if not data:
        logger.warning("No data to save for %s.", asset_name)
        return False

    stocks_dir = STOCKS_PATH / asset_name
    os.makedirs(stocks_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    file_path = stocks_dir / f"{timestamp}_{asset_name}_raw.pq"

    if file_path.exists():
        logger.info(
            "File already exists for %s this hour, skipping save: %s",
            asset_name,
            file_path
        )
        return True
    try:
        # Identify the time-series field dynamically.
        time_series_key = next(
            (key for key in data if key.startswith("Time Series")),
            None
        )
        if time_series_key is None:
            logger.error("No time-series field found for %s.", asset_name)
            return False
        metadata = data.get("Meta Data", {})
        time_series = data.get(time_series_key, {})

        if not isinstance(time_series, dict) or not time_series:
            logger.error("Invalid or empty time-series data for %s.", asset_name)
            return False
        # Preserve metadata and observation dates.
        records = [
            {
                "Meta Data": value,
                time_series_key: None
            }
            for value in metadata.values()
        ]
        records.extend(
            {
                "Meta Data": date,
                time_series_key: values
            }
            for date, values in time_series.items()
        )
        df_raw = pl.DataFrame(records)
        df_raw.write_parquet(file_path)
        logger.info("Saved raw Parquet file to: %s", file_path)
        return True
    except Exception:
        logger.exception("Failed to save raw data for %s.", asset_name)
        return False

    
def run_alphavantage_stock_bronze(configs: dict) -> bool:
    logger.info("Initializing stock ingestion cycle...")

    success = True
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    for stock_name, stock_config in configs.items():

        expected_file = (
            STOCKS_PATH
            / stock_name
            / f"{timestamp}_{stock_name}_raw.pq"
        )

        if expected_file.exists():
            logger.info(
                "[API SKIPPED] Data for %s already pulled this hour.",
                stock_name
            )
            continue

        try:
            raw_payload = extract_raw_stock(stock_name, stock_config)

            if not raw_payload:
                success = False
                continue

            if not save_raw_to_disk(stock_name, raw_payload):
                success = False

        except AlphaVantageQuotaExceededError as e:
            logger.warning(
                "Local API quota exhausted while processing %s. "
                "Next request available in %.0f seconds.",
                stock_name,
                e.retry_after
            )
            success = False
            break

        except AlphaVantageRateLimitError:
            logger.exception(
                "Alpha Vantage rate limit reached while processing %s.",
                stock_name
            )
            success = False
            break

        except Exception:
            logger.exception(
                "Unexpected error while processing %s.",
                stock_name
            )
            success = False

    if success:
        logger.info("Stock extraction completed successfully.")
    else:
        logger.warning("Stock extraction completed with failures.")

    return success

if __name__ == "__main__":
    stock_config_path = CONFIG_PATH / "alphavantage_stock_config.json"

    try:
        with open(stock_config_path, "r", encoding="utf-8") as file:
            stock_config_data = json.load(file)
        local_configs = stock_config_data.get("stocks", {})

        if not local_configs:
            print("❌ No stock configurations found.")
        else:
            run_alphavantage_stock_bronze(configs=local_configs)

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {stock_config_path}")
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON configuration: {e}")