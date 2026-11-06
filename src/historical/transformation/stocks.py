import polars as pl

from src.historical.paths import (
    HISTORICAL_STOCKS_DATA_PATH,
    HISTORICAL_SILVER_STOCKS_PATH,
)
from src.historical.validation.stocks import validate_historical_stock


def transform_historical_stocks(stock_symbols: list[str]) -> bool:
    """
    Consolidate Twelve Data Bronze stock batches into Silver Parquet files.
    """

    HISTORICAL_SILVER_STOCKS_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    overall_success = True

    for symbol in stock_symbols:
        print(f"\n[HISTORICAL SILVER] Processing {symbol}...")

        stock_path = HISTORICAL_STOCKS_DATA_PATH / symbol
        output_path = HISTORICAL_SILVER_STOCKS_PATH / f"{symbol}_silver.pq"

        try:
            raw_files = sorted(stock_path.glob(f"{symbol}_*_raw.pq"))

            if not raw_files:
                print(
                    f"[HISTORICAL SILVER] No Bronze files found for {symbol}."
                )
                overall_success = False
                continue

            dataframes = [
                pl.read_parquet(file)
                for file in raw_files
            ]

            df = pl.concat(dataframes)

            df = (
                df
                .unique(subset=["date"], keep="last")
                .sort("date")
            )

            validate_historical_stock(df, symbol)

            df.write_parquet(output_path)

            print(
                f"[HISTORICAL SILVER] {symbol} saved successfully "
                f"({df.height:,} rows)."
            )

        except Exception as e:
            print(
                f"[HISTORICAL SILVER] Failed to process {symbol}: "
                f"{type(e).__name__}: {e}"
            )
            overall_success = False

    return overall_success
