
from src.historical.paths import HISTORICAL_COMMODITIES_DATA_PATH


def ingest_historical_commodities(
    provider,
    symbols: list[str],
    start_date: str,
    end_date: str
) -> None:

    HISTORICAL_COMMODITIES_DATA_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    for symbol in symbols:
        try:
            print(f"[HISTORICAL COMMODITIES] Downloading {symbol}...")

            df = provider.get_commodity_history(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df.is_empty():
                print(
                    f"[HISTORICAL COMMODITIES] "
                    f"No data returned for {symbol}."
                )
                continue

            output_path = (
                HISTORICAL_COMMODITIES_DATA_PATH
                / f"{symbol}_historical_raw.pq"
            )

            df.write_parquet(output_path)

            print(
                f"[HISTORICAL COMMODITIES] {symbol} saved successfully "
                f"({df.height} rows)."
            )

        except Exception as e:
            print(
                f"[HISTORICAL COMMODITIES] "
                f"Failed to ingest {symbol}: {e}"
            )
