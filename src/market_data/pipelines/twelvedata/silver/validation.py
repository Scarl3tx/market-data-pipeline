
import polars as pl


SILVER_STOCK_SCHEMA = {
    "asset_name": pl.String,
    "date": pl.Datetime("ns"),
    "year": pl.Int32,
    "month": pl.Int8,
    "day": pl.Int8,
    "day_of_week": pl.Int8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "rsi_14": pl.Float64,
    "sma_50": pl.Float64,
    "sma_200": pl.Float64,
}

REQUIRED_COLUMNS = [
    "asset_name",
    "date",
    "year",
    "month",
    "day",
    "day_of_week",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

INDICATOR_COLUMNS = [
    "rsi_14",
    "sma_50",
    "sma_200",
]


def validate_stock_silver(df: pl.DataFrame) -> bool:

    expected_columns = list(SILVER_STOCK_SCHEMA.keys())

    if df.columns != expected_columns:
        raise ValueError(
            f"Silver column mismatch. "
            f"Expected {expected_columns}, got {df.columns}"
        )

    for column, expected_type in SILVER_STOCK_SCHEMA.items():
        actual_type = df.schema[column]

        if actual_type != expected_type:
            raise TypeError(
                f"Invalid type for {column}. "
                f"Expected {expected_type}, got {actual_type}"
            )

    if df["date"].n_unique() != df.height:
        raise ValueError("Duplicate dates found in Silver data.")

    # Required columns must never contain nulls.
    null_counts = df.select(
        [
            pl.col(column).null_count().alias(column)
            for column in REQUIRED_COLUMNS
        ]
    )

    if any(null_counts.row(0)):
        raise ValueError("Null values found in required Silver columns.")

    if not df["date"].is_sorted():
        raise ValueError("Silver dates must be sorted.")

    # Validate OHLC relationships.
    invalid_ohlc = df.filter(
        (pl.col("high") < pl.max_horizontal(
            "open",
            "close",
            "low",
        ))
        |
        (pl.col("low") > pl.min_horizontal(
            "open",
            "close",
            "high",
        ))
    )

    if not invalid_ohlc.is_empty():
        raise ValueError("Invalid OHLC values found.")

    # Null indicators are permitted. Only validate populated values.
    invalid_rsi = df.filter(
        pl.col("rsi_14").is_not_null()
        & (
            ~pl.col("rsi_14").is_finite()
            | (pl.col("rsi_14") < 0)
            | (pl.col("rsi_14") > 100)
        )
    )

    if not invalid_rsi.is_empty():
        raise ValueError("Invalid RSI values found.")

    for column in ("sma_50", "sma_200"):
        invalid_sma = df.filter(
            pl.col(column).is_not_null()
            & ~pl.col(column).is_finite()
        )

        if not invalid_sma.is_empty():
            raise ValueError(f"Invalid {column} values found.")

    return True
