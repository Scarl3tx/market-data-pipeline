
from datetime import date

import polars as pl


REQUIRED_COLUMNS = {
    "asset_name",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


def validate_stock_data(
    df: pl.DataFrame,
    today: date | None = None,
) -> dict:
    """
    Validate stock records before Gold ingestion.

    Invalid rows are retained with a validation_errors column.
    The input DataFrame is not modified.
    """

    if today is None:
        today = date.today()

    if df.is_empty():
        return {
            "valid": df.clone(),
            "invalid": df.clone(),
            "rejected_rows": 0,
            "reasons": {},
        }

    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Missing required stock columns: "
            + ", ".join(sorted(missing_columns))
        )

    open_price = pl.col("open").cast(pl.Float64, strict=False)
    high_price = pl.col("high").cast(pl.Float64, strict=False)
    low_price = pl.col("low").cast(pl.Float64, strict=False)
    close_price = pl.col("close").cast(pl.Float64, strict=False)
    volume = pl.col("volume").cast(pl.Float64, strict=False)

    stock_date = pl.col("date").cast(
        pl.Date,
        strict=False,
    )

    checks = {
        "invalid_symbol": (
            pl.col("asset_name").is_null()
            | (
                pl.col("asset_name")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                == ""
            )
        ),

        "invalid_date": (
            pl.col("date").is_null()
            | stock_date.is_null()
            | (stock_date > pl.lit(today))
        ),

        "invalid_open": (
            open_price.is_null()
            | ~open_price.is_finite()
            | (open_price <= 0)
        ),

        "invalid_high": (
            high_price.is_null()
            | ~high_price.is_finite()
            | (high_price <= 0)
        ),

        "invalid_low": (
            low_price.is_null()
            | ~low_price.is_finite()
            | (low_price <= 0)
        ),

        "invalid_close": (
            close_price.is_null()
            | ~close_price.is_finite()
            | (close_price <= 0)
        ),

        "invalid_volume": (
            volume.is_null()
            | ~volume.is_finite()
            | (volume < 0)
            | ((volume % 1) != 0)
        ),

        "invalid_ohlc_relationship": (
            (high_price < open_price)
            | (high_price < close_price)
            | (high_price < low_price)
            | (low_price > open_price)
            | (low_price > close_price)
        ),
    }

    reason_columns = list(checks.keys())

    flagged_df = df.with_columns(
        [
            condition
            .fill_null(True)
            .alias(reason)
            for reason, condition in checks.items()
        ]
    )

    invalid_condition = pl.any_horizontal(
        *[
            pl.col(column)
            for column in reason_columns
        ]
    )

    # Use the original expressions directly, not pl.col().
    error_expressions = [
        pl.when(condition.fill_null(True))
        .then(pl.lit(reason))
        .otherwise(pl.lit(None, dtype=pl.String))
        for reason, condition in checks.items()
    ]

    flagged_df = flagged_df.with_columns(
        pl.concat_str(
            error_expressions,
            separator=", ",
            ignore_nulls=True,
        ).alias("validation_errors")
    )

    valid_df = (
        flagged_df
        .filter(~invalid_condition)
        .drop(reason_columns + ["validation_errors"])
    )

    invalid_df = (
        flagged_df
        .filter(invalid_condition)
        .drop(reason_columns)
    )

    reason_counts = {
        reason: int(
            flagged_df.select(
                pl.col(reason).sum()
            ).item() or 0
        )
        for reason in reason_columns
    }

    reason_counts = {
        reason: count
        for reason, count in reason_counts.items()
        if count > 0
    }

    return {
        "valid": valid_df,
        "invalid": invalid_df,
        "rejected_rows": invalid_df.height,
        "reasons": reason_counts,
    }
