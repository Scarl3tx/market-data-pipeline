import json

from src.alphavantage_trackers.paths import CONFIG_PATH
from src.historical.transformation.stocks import transform_historical_stocks


STOCK_CONFIG_PATH = CONFIG_PATH / "alphavantage_stock_config.json"


def load_stock_symbols() -> list[str]:
    with open(STOCK_CONFIG_PATH, "r") as file:
        config = json.load(file)

    return list(config["stocks"].keys())


def run_historical_silver_pipeline() -> bool:
    print("\n========== HISTORICAL SILVER PIPELINE STARTED ==========")

    try:
        stock_symbols = load_stock_symbols()

        success = transform_historical_stocks(stock_symbols)

        if success:
            print("\n[HISTORICAL SILVER] All stock transformations completed.")
        else:
            print("\n[HISTORICAL SILVER] Some stock transformations failed.")

        return success

    except Exception as e:
        print(
            f"\n[HISTORICAL SILVER] Pipeline failed: "
            f"{type(e).__name__}: {e}"
        )
        return False

    finally:
        print("\n========== HISTORICAL SILVER PIPELINE FINISHED ==========")


if __name__ == "__main__":
    run_historical_silver_pipeline()
