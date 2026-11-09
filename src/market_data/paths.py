
import os
from pathlib import Path

# ============================================================
# Project root and configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = Path(
    os.getenv(
        "MARKET_DATA_CONFIG_DIR",
        str(PROJECT_ROOT / "configs"),
    )
)

STOCKS_CONFIG_PATH = CONFIG_PATH / "stocks_config.json"

STOCKS_SCHEMA_PATH = (
    PROJECT_ROOT
    / "src"
    / "market_data"
    / "database"
    / "schemas"
    / "stocks.json"
)


# ============================================================
# Environment files
# ============================================================

TWELVE_DATA_ENV_PATH = PROJECT_ROOT / "twelvedata_api.env"
ALPHAVANTAGE_ENV_PATH = PROJECT_ROOT / "alphavantage_api.env"
MARKETAUX_ENV_PATH = PROJECT_ROOT / "marketaux_api.env"


# ============================================================
# Data directories
# ============================================================

# Local development defaults to PROJECT_ROOT/data.
# AWS Lambda overrides this with /tmp/market_data.

DATA_PATH = Path(
    os.getenv(
        "MARKET_DATA_DATA_DIR",
        str(PROJECT_ROOT / "data"),
    )
)

BRONZE_PATH = DATA_PATH / "bronze"
SILVER_PATH = DATA_PATH / "silver"
GOLD_PATH = DATA_PATH / "gold"
QUARANTINE_PATH = DATA_PATH / "quarantine"

DATABASE_DIR = DATA_PATH / "database"
STATE_PATH = DATA_PATH / "state"


# ============================================================
# Bronze: provider-specific raw data
# ============================================================

TWELVEDATA_BRONZE_PATH = BRONZE_PATH / "twelvedata"

# TwelveData stock data is stored under:
# bronze/twelvedata/stocks/{SYMBOL}/

TWELVEDATA_STOCKS_BRONZE_PATH = (
    TWELVEDATA_BRONZE_PATH / "stocks"
)

TWELVEDATA_STOCKS_BRONZE_S3_PREFIX = (
    "bronze/twelvedata/stocks"
)

ALPHAVANTAGE_BRONZE_PATH = BRONZE_PATH / "alphavantage"
MARKETAUX_BRONZE_PATH = BRONZE_PATH / "marketaux"

ALPHAVANTAGE_BRONZE_NEWS_PATH = (
    ALPHAVANTAGE_BRONZE_PATH / "news"
)

MARKETAUX_BRONZE_NEWS_PATH = (
    MARKETAUX_BRONZE_PATH / "news"
)


# ============================================================
# Silver: standardised data
# ============================================================

STOCKS_SILVER_PATH = SILVER_PATH / "stocks"

# Canonical AlphaVantage Silver location:
# silver/alphavantage/news/

ALPHAVANTAGE_SILVER_NEWS_PATH = (
    SILVER_PATH / "alphavantage" / "news"
)

ALPHAVANTAGE_NEWS_SILVER_FILE = (
    ALPHAVANTAGE_SILVER_NEWS_PATH / "news_silver.pq"
)

MARKETAUX_SILVER_PATH = SILVER_PATH / "marketaux"

MARKETAUX_NEWS_SILVER_PATH = (
    MARKETAUX_SILVER_PATH / "news"
)

MARKETAUX_NEWS_SILVER_FILE = (
    MARKETAUX_NEWS_SILVER_PATH / "news_silver.pq"
)


# ============================================================
# Gold: analytics-ready data
# ============================================================

STOCKS_GOLD_PATH = GOLD_PATH / "stocks"


# ============================================================
# Quarantine: rejected records
# ============================================================

TWELVEDATA_QUARANTINE_PATH = (
    QUARANTINE_PATH / "twelvedata"
)

TWELVEDATA_STOCKS_QUARANTINE_PATH = (
    TWELVEDATA_QUARANTINE_PATH / "stocks"
)


# ============================================================
# Database
# ============================================================

DATABASE_PATH = Path(
    os.getenv(
        "MARKET_DATA_DATABASE_PATH",
        str(DATABASE_DIR / "market_data.duckdb"),
    )
)

DATABASE_DIR = DATABASE_PATH.parent


# ============================================================
# Persistent state
# ============================================================

TWELVEDATA_RATE_LIMIT_STATE = (
    STATE_PATH / "twelvedata_rate_limit.json"
)

TWELVEDATA_HOURLY_STATE = (
    STATE_PATH / "twelvedata_hourly_state.json"
)

ALPHAVANTAGE_API_USAGE_PATH = (
    STATE_PATH / "alphavantage_api_usage.sqlite3"
)

ALPHAVANTAGE_NEWS_STATE_PATH = (
    STATE_PATH / "alphavantage_news_state.json"
)

MARKETAUX_API_USAGE_PATH = (
    STATE_PATH / "marketaux_api_usage.sqlite3"
)

MARKETAUX_RATE_LIMIT_STATE_PATH = (
    STATE_PATH / "marketaux_rate_limit.json"
)

MARKETAUX_NEWS_STATE_PATH = (
    STATE_PATH / "marketaux_news_state.json"
)

PIPELINE_LOCK_PATH = STATE_PATH / "pipeline_execution.lock"
