
import json
from datetime import date

from src.alphavantage_trackers.paths import CONFIG_PATH
from src.historical.providers.twelve_data import TwelveDataProvider
from src.historical.ingestion.orchestrator import run_historical_ingestion

STOCK_CONFIG_PATH = CONFIG_PATH / "alphavantage_stock_config.json"

def load_stock_config():
    with open(STOCK_CONFIG_PATH, "r") as file:
        config = json.load(file)

    return config["stocks"]


def run_historical_pipeline():

    print("\n========== HISTORICAL PIPELINE STARTED ==========")

    # Load configured stock symbols
    stock_config = load_stock_config()
    stock_symbols = list(stock_config.keys())

    # Commodity symbols (placeholder)
    commodity_symbols = []

    # Historical date range
    start_date = "1999-01-01"
    end_date = "2026-11-03"

    # Initialise historical data provider
    provider = TwelveDataProvider()

    # Execute ingestion
    result = run_historical_ingestion(
    provider=provider,
    stock_symbols=stock_symbols,
    commodity_symbols=commodity_symbols,
    start_date=start_date,
    end_date=end_date
    )

    print("\n========== HISTORICAL PIPELINE FINISHED ==========")
    return result

if __name__ == "__main__":
    run_historical_pipeline()
