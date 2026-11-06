
import logging
from datetime import date, datetime

import polars as pl
import pandas_market_calendars as mcal


logger = logging.getLogger(__name__)

nyse = mcal.get_calendar("NYSE")


# ---------------------------------------------------------------------------
# Percentage changes and trading-day gap validation
# ---------------------------------------------------------------------------

def calculate_percentage_changes(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    required = {"asset_name", "close", "date"}
    if not required.issubset(df.columns):
        logger.warning(
            f"Cannot calculate percentage changes; missing "
            f"{sorted(required.difference(df.columns))}."
        )
        return df

    result = df.sort(["asset_name", "date"]).with_columns(
        pl.col("close")
        .shift(1)
        .over("asset_name")
        .alias("_prev_close")
    )

    result = result.with_columns(
        (
            (pl.col("close") - pl.col("_prev_close"))
            / pl.col("_prev_close")
            * 100
        ).alias("pct_change")
    )

    # Build a NYSE session index once instead of calling the calendar
    # once per row.
    date_values = result["date"].to_list()

    python_dates = [
        value.date() if isinstance(value, datetime) else value
        for value in date_values
    ]

    start_date = min(python_dates)
    end_date = max(python_dates)

    valid_sessions = nyse.valid_days(
        start_date=start_date,
        end_date=end_date,
    )

    session_lookup = {
        session.date(): index
        for index, session in enumerate(valid_sessions)
    }

    session_indices = [
        session_lookup.get(value)
        for value in python_dates
    ]

    result = result.with_columns(
        pl.Series(
            "_session_index",
            session_indices,
            dtype=pl.Int64,
        )
    ).with_columns(
        (
            pl.col("_session_index")
            - pl.col("_session_index").shift(1).over("asset_name")
        ).alias("_trading_day_gap")
    )

    gap_rows = result.filter(
        pl.col("_trading_day_gap").is_not_null()
        & (pl.col("_trading_day_gap") > 1)
    ).height

    result = result.with_columns(
        pl.when(
            pl.col("_trading_day_gap").is_null()
            | (pl.col("_trading_day_gap") <= 1)
        )
        .then(pl.col("pct_change"))
        .otherwise(None)
        .alias("pct_change")
    )
    gap_rows = result.filter(pl.col("_trading_day_gap") > 1)

    if not gap_rows.is_empty():
        logger.warning(
            "Data gaps detected:\n%s",
            gap_rows.select(
                "asset_name",
                "date",
                "_trading_day_gap"
            )
        )
        result = result.with_columns(
        pl.when(pl.col("_trading_day_gap") > 1)
        .then(None)
        .otherwise(pl.col("pct_change"))
        .alias("pct_change")
        )
        logger.warning(
        "DATA GAP FIREWALL: Set pct_change to null on %d rows "
        "due to missing NYSE trading sessions.",
        gap_rows.height
        )
    return result.drop(
        [
            "_prev_close",
            "_session_index",
            "_trading_day_gap",
        ]
    )


# ---------------------------------------------------------------------------
# Moving averages and rolling volatility
# ---------------------------------------------------------------------------

def calculate_technical_indicators(
    df: pl.DataFrame,
) -> pl.DataFrame:

    required = {"asset_name", "date", "close", "pct_change"}

    if df.is_empty() or not required.issubset(df.columns):
        return df

    result = df.sort(["asset_name", "date"])

    return result.with_columns(
        pl.col("close")
        .rolling_mean(window_size=7, min_samples=7)
        .over("asset_name")
        .alias("sma_7"),

        pl.col("close")
        .rolling_mean(window_size=30, min_samples=30)
        .over("asset_name")
        .alias("sma_30"),

        pl.col("pct_change")
        .rolling_std(window_size=7, min_samples=7)
        .over("asset_name")
        .alias("volatility_7"),

        pl.col("pct_change")
        .rolling_std(window_size=30, min_samples=30)
        .over("asset_name")
        .alias("volatility_30"),
    )


# ---------------------------------------------------------------------------
# RSI (Wilder-style exponential smoothing)
# ---------------------------------------------------------------------------

def calculate_rsi(
    df: pl.DataFrame,
    periods: list[int],
) -> pl.DataFrame:

    if df.is_empty():
        return df

    required = {"asset_name", "date", "close"}

    if not required.issubset(df.columns):
        return df

    if not periods or any(
        not isinstance(period, int) or period <= 0
        for period in periods
    ):
        raise ValueError("RSI periods must be positive integers.")

    result = df.sort(["asset_name", "date"])

    result = result.with_columns(
        pl.col("close")
        .diff()
        .over("asset_name")
        .alias("_delta")
    ).with_columns(
        pl.col("_delta").clip(lower_bound=0).alias("_gain"),
        (-pl.col("_delta").clip(upper_bound=0)).alias("_loss"),
    )

    for period in periods:
        result = result.with_columns(
            pl.col("_gain")
            .ewm_mean(
                alpha=1 / period,
                adjust=False,
                min_samples=period,
            )
            .over("asset_name")
            .alias(f"_avg_gain_{period}"),

            pl.col("_loss")
            .ewm_mean(
                alpha=1 / period,
                adjust=False,
                min_samples=period,
            )
            .over("asset_name")
            .alias(f"_avg_loss_{period}"),
        )

        result = result.with_columns(
            pl.when(
                (pl.col(f"_avg_loss_{period}") == 0)
                & (pl.col(f"_avg_gain_{period}") == 0)
            )
            .then(50.0)
            .when(pl.col(f"_avg_loss_{period}") == 0)
            .then(100.0)
            .otherwise(
                100
                - (
                    100
                    / (
                        1
                        + pl.col(f"_avg_gain_{period}")
                        / pl.col(f"_avg_loss_{period}")
                    )
                )
            )
            .alias(f"rsi_{period}")
        )

    temporary_columns = [
        "_delta",
        "_gain",
        "_loss",
        *[
            name
            for period in periods
            for name in (
                f"_avg_gain_{period}",
                f"_avg_loss_{period}",
            )
        ],
    ]

    return result.drop(temporary_columns)


# ---------------------------------------------------------------------------
# Combined technical indicator calculation
# ---------------------------------------------------------------------------

def calculate_all_indicators(
    df: pl.DataFrame,
    rsi_periods: list[int] | None = None,
) -> pl.DataFrame:

    if df.is_empty():
        return df

    if rsi_periods is None:
        rsi_periods = [7, 14, 21]

    result = calculate_percentage_changes(df)

    result = calculate_technical_indicators(result)

    result = calculate_rsi(
        result,
        periods=rsi_periods,
    )

    return result
