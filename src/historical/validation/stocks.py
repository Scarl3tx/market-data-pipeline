import polars as pl
import json
from src.alphavantage_trackers.paths import CONFIG_PATH

STOCK_SCHEMA_PATH = CONFIG_PATH / "alphavantage_stock_schemas.json"
def load_stock_schema():
    with open(STOCK_SCHEMA_PATH, "r") as file:
        schema = json.load(file)
    return schema["equity_daily"]

def validate_historical_stock(
    df: pl.DataFrame,
    symbol: str
) -> bool:

    expected_schema = load_stock_schema()

    # Map JSON schema types to Polars types
    type_mapping = {
        "object": pl.String,
        "datetime64[ns]": pl.Datetime("ns"),
        "float64": pl.Float64,
        "int64": pl.Int64
    }

    # Check columns
    expected_columns = list(expected_schema.keys())

    if df.columns != expected_columns:
        raise ValueError(
            f"{symbol}: Column mismatch. "
            f"Expected {expected_columns}, got {df.columns}"
        )

    # Check data types
    for column, expected_type in expected_schema.items():

        polars_type = type_mapping.get(expected_type)

        if polars_type is None:
            raise ValueError(
                f"Unsupported schema type: {expected_type}"
            )

        if df.schema[column] != polars_type:
            raise TypeError(
                f"{symbol}: Incorrect type for {column}. "
                f"Expected {polars_type}, got {df.schema[column]}"
            )

    # Check duplicate dates
    if df["date"].n_unique() != df.height:
        raise ValueError(f"{symbol}: Duplicate dates detected.")

    # Check null values
    null_counts = df.null_count().row(0)

    if any(null_counts):
        raise ValueError(
            f"{symbol}: Null values detected: {null_counts}"
        )

    # Check date ordering
    if not df["date"].is_sorted():
        raise ValueError(
            f"{symbol}: Dates are not sorted ascending."
        )

    # Check OHLC consistency
    invalid_ohlc = df.filter(
        (pl.col("high") < pl.max_horizontal("open", "close", "low")) |
        (pl.col("low") > pl.min_horizontal("open", "close", "high"))
    )

    if not invalid_ohlc.is_empty():
        raise ValueError(
            f"{symbol}: Invalid OHLC relationships detected."
        )

    print(f"[HISTORICAL STOCKS] {symbol} validation passed.")
    return True
