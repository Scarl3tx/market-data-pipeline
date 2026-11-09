
import polars as pl


def _calculate_rsi(
    closes: list[float],
    period: int = 14,
) -> list[float | None]:
    """Calculate RSI using Wilder's smoothing method."""

    result: list[float | None] = [None] * len(closes)

    if len(closes) <= period:
        return result

    gains: list[float] = []
    losses: list[float] = []

    for previous, current in zip(closes, closes[1:]):
        change = current - previous
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    def calculate_value(gain: float, loss: float) -> float:
        if gain == 0 and loss == 0:
            return 50.0

        if loss == 0:
            return 100.0

        if gain == 0:
            return 0.0

        relative_strength = gain / loss
        return 100.0 - (100.0 / (1.0 + relative_strength))

    result[period] = calculate_value(
        average_gain,
        average_loss,
    )

    for index in range(period + 1, len(closes)):
        average_gain = (
            average_gain * (period - 1) + gains[index - 1]
        ) / period

        average_loss = (
            average_loss * (period - 1) + losses[index - 1]
        ) / period

        result[index] = calculate_value(
            average_gain,
            average_loss,
        )

    return result


def transform_stock_silver(
    df: pl.DataFrame,
) -> pl.DataFrame:

    if df.is_empty():
        return pl.DataFrame()

    df = (
        df.with_columns(
            pl.col("date").cast(pl.Datetime("ns"))
        )
        .sort("date")
    )

    df = df.with_columns([
        pl.col("date").dt.year().cast(pl.Int32).alias("year"),
        pl.col("date").dt.month().cast(pl.Int8).alias("month"),
        pl.col("date").dt.day().cast(pl.Int8).alias("day"),
        pl.col("date").dt.weekday().cast(pl.Int8).alias("day_of_week"),

        pl.col("close")
        .rolling_mean(window_size=50)
        .cast(pl.Float64)
        .alias("sma_50"),

        pl.col("close")
        .rolling_mean(window_size=200)
        .cast(pl.Float64)
        .alias("sma_200"),
    ])

    rsi_values = _calculate_rsi(
        df["close"].to_list(),
        period=14,
    )

    df = df.with_columns(
        pl.Series(
            "rsi_14",
            rsi_values,
            dtype=pl.Float64,
        )
    )

    return (
        df.select([
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
            "rsi_14",
            "sma_50",
            "sma_200",
        ])
        .sort("date")
    )
