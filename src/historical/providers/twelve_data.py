
import os
import requests
import polars as pl

from dotenv import load_dotenv

from src.historical.providers.base import HistoricalDataProvider
from src.historical.paths import (
    PROJECT_ROOT,
    TWELVE_DATA_RATE_LIMIT_STATE
)
from src.historical.ingestion.twelvedata_rate_limiter import (
    TwelveDataRateLimiter
)
load_dotenv(PROJECT_ROOT / "twelvedata_api.env")
API_KEY = os.getenv("TWELVE_DATA_API_KEY")

class TwelveDataProvider(HistoricalDataProvider):

    BASE_URL = "https://api.twelvedata.com/time_series"

    def __init__(self):
        if not API_KEY:
            raise ValueError(
                "TWELVE_DATA_API_KEY not found in environment variables."
            )

        self.rate_limiter = TwelveDataRateLimiter(
            state_path=TWELVE_DATA_RATE_LIMIT_STATE
        )

    def get_stock_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pl.DataFrame:

        params = {
            "symbol": symbol,
            "interval": "1day",
            "start_date": start_date,
            "end_date": end_date,
            "outputsize": 5000,
            "adjust": "none",
            "order": "asc",
            "apikey": API_KEY,
            "format": "JSON"
        }

        # Reserve an API credit and wait for an available request slot
        self.rate_limiter.wait_for_slot()

        response = requests.get(
            self.BASE_URL,
            params=params,
            timeout=30
        )

        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
            raise RuntimeError(
                f"Twelve Data returned an invalid JSON response for {symbol}."
            )
        if response.status_code == 400:
            message = payload.get("message", "") if isinstance(payload, dict) else ""
            if "no data" in message.lower():
                print(
                    f"[TWELVE DATA] No historical data available "
                    f"for {symbol} in the requested date range."
                )
                return pl.DataFrame()
            raise RuntimeError(
                f"Twelve Data returned HTTP 400 for {symbol}."
            )
        response.raise_for_status()
        if isinstance(payload, dict) and payload.get("status") == "error":
            raise RuntimeError(
                f"Twelve Data API error for {symbol}: "
                f"{payload.get('message', 'Unknown API error')}"
            )

        if not payload.get("values"):
            return pl.DataFrame(
                schema={
                    "asset_name": pl.String,
                    "date": pl.Datetime("ns"),
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64
                }
            )

        df = pl.DataFrame(payload["values"])

        df = df.rename({"datetime": "date"})

        df = df.with_columns([
            pl.lit(symbol).alias("asset_name"),
            pl.col("date")
                .str.to_datetime(format="%Y-%m-%d")
                .cast(pl.Datetime("ns")),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64)
        ])

        df = df.select([
            "asset_name",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ])

        return df


# if __name__ == "__main__":

#     provider = TwelveDataProvider()

#     df = provider.get_stock_history(
#     symbol="NVDA",
#     start_date="1999-01-01",
#     end_date="2006-12-14"
#     )

#     print(df)
#     print(f"\nRows returned: {df.height}")
#     print(f"Earliest date: {df['date'].min()}")
#     print(f"Latest date: {df['date'].max()}")
