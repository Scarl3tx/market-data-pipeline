import requests
import polars as pl
from src.historical.paths import HISTORICAL_STOCKS_DATA_PATH
from src.historical.validation.stocks import validate_historical_stock
from src.historical.ingestion.exceptions import TwelveDataDailyLimitError
from datetime import date, timedelta

def ingest_historical_stock_batch(
    provider,
    symbol: str,
    start_date: str,
    end_date: str
) -> dict:

    HISTORICAL_STOCKS_DATA_PATH.mkdir(parents=True, exist_ok=True)

    try:
        print(
            f"[HISTORICAL STOCKS] Downloading {symbol}: "
            f"{start_date} to {end_date}..."
        )

        df = provider.get_stock_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

        # Handle empty API response
        if df.is_empty():
            print(
                f"[HISTORICAL STOCKS] No data returned for {symbol}."
            )

            return {
                "symbol": symbol,
                "success": True,
                "rows": 0,
                "earliest_date": None,
                "latest_date": None,
                "has_data": False
            }

        # Validate historical data
        validate_historical_stock(df, symbol)

        # Determine actual date coverage
        earliest_date = df["date"].min().strftime("%Y%m%d")
        latest_date = df["date"].max().strftime("%Y%m%d")

        # Construct output path
        output_path = (
            HISTORICAL_STOCKS_DATA_PATH
            / symbol
            / f"{symbol}_{earliest_date}_{latest_date}_raw.pq"
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Skip if the batch already exists
        if output_path.exists():
            print(
                f"[HISTORICAL STOCKS] {symbol} batch already exists. "
                "Skipping."
            )

            return {
                "symbol": symbol,
                "success": True,
                "rows": df.height,
                "earliest_date": earliest_date,
                "latest_date": latest_date,
                "has_data": True,
                "skipped": True
            }
        # Save historical data
        df.write_parquet(output_path)

        print(
            f"[HISTORICAL STOCKS] {symbol} saved successfully "
            f"({df.height} rows)."
        )

        return {
            "symbol": symbol,
            "success": True,
            "rows": df.height,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "has_data": True
        }

    except TwelveDataDailyLimitError:
        # Allow the orchestrator to stop and resume later
        raise

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else "Unknown"

        print(
            f"[HISTORICAL STOCKS] HTTP error for {symbol}: "
            f"status {status_code}"
        )

        return {
            "symbol": symbol,
            "success": False,
            "rows": 0,
            "earliest_date": None,
            "latest_date": None,
            "has_data": False,
            "error": f"HTTP {status_code}"
        }

def update_historical_stock(
    provider,
    symbol: str,
    buffer_days: int = 30
) -> dict:
    """
    Update historical stock data only when the latest stored date
    is more than buffer_days behind today.
    """

    stock_path = HISTORICAL_STOCKS_DATA_PATH / symbol

    if not stock_path.exists():
        print(
            f"[HISTORICAL UPDATE] No historical data found for {symbol}."
        )
        return {
            "symbol": symbol,
            "success": False,
            "updated": False
        }

    raw_files = list(stock_path.glob(f"{symbol}_*_raw.pq"))

    if not raw_files:
        return {
            "symbol": symbol,
            "success": False,
            "updated": False
        }

    latest_dates = []

    for file in raw_files:
        df = pl.read_parquet(file, columns=["date"])
        latest_dates.append(df["date"].max())

    latest_date = max(latest_dates).date()
    today = date.today()

    gap = (today - latest_date).days

    if gap <= buffer_days:
        print(
            f"[HISTORICAL UPDATE] {symbol} is up to date "
            f"(latest: {latest_date}, gap: {gap} days). Skipping."
        )

        return {
            "symbol": symbol,
            "success": True,
            "updated": False,
            "latest_date": latest_date.isoformat()
        }

    update_start = latest_date + timedelta(days=1)

    print(
        f"[HISTORICAL UPDATE] {symbol} requires updating. "
        f"Latest date: {latest_date}, gap: {gap} days."
    )

    result = ingest_historical_stock_batch(
        provider=provider,
        symbol=symbol,
        start_date=update_start.isoformat(),
        end_date=today.isoformat()
    )

    return {
        **result,
        "updated": result["success"] and result["has_data"]
    }