
import polars as pl

from market_data.database.connection import get_database_connection
from market_data.database.tables import (
    create_marketaux_news_table,
    create_market_features_table,
    create_news_table,
    create_stocks_table,
)
from market_data.database.write_lock import DATABASE_WRITE_LOCK
from market_data.storage.base import StorageBackend


MARKET_FEATURES_GOLD_KEY = "gold/market_features_daily.pq"


def load_stocks_to_gold(
    df: pl.DataFrame,
    connection=None,
) -> int:
    if df.is_empty():
        return 0

    with DATABASE_WRITE_LOCK:
        owns_connection = connection is None

        if owns_connection:
            connection = get_database_connection()

        try:
            create_stocks_table(connection)

            indicator_columns = {
                "rsi_14": pl.Float64,
                "sma_50": pl.Float64,
                "sma_200": pl.Float64,
            }

            missing_columns = [
                pl.lit(None, dtype=dtype).alias(column)
                for column, dtype in indicator_columns.items()
                if column not in df.columns
            ]

            if missing_columns:
                df = df.with_columns(missing_columns)

            connection.register("silver_batch", df.to_arrow())

            try:
                connection.execute("""
                    INSERT INTO stocks_daily (
                        asset_name, date, year, month, day,
                        day_of_week, open, high, low, close,
                        volume, rsi_14, sma_50, sma_200
                    )
                    SELECT
                        asset_name, date, year, month, day,
                        day_of_week, open, high, low, close,
                        volume, rsi_14, sma_50, sma_200
                    FROM silver_batch
                    ON CONFLICT (asset_name, date)
                    DO UPDATE SET
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        day_of_week = EXCLUDED.day_of_week,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        rsi_14 = EXCLUDED.rsi_14,
                        sma_50 = EXCLUDED.sma_50,
                        sma_200 = EXCLUDED.sma_200
                """)
            finally:
                connection.unregister("silver_batch")

            return df.height

        finally:
            if owns_connection:
                connection.close()


def load_news_to_gold(
    df: pl.DataFrame,
    connection=None,
) -> int:
    if df.is_empty():
        return 0

    with DATABASE_WRITE_LOCK:
        owns_connection = connection is None

        if owns_connection:
            connection = get_database_connection()

        try:
            create_news_table(connection)
            connection.register("news_batch", df)

            try:
                connection.execute("""
                    INSERT INTO news_daily
                    SELECT
                        ticker, date, article_count,
                        avg_sentiment_score, bullish_count,
                        neutral_count, bearish_count
                    FROM news_batch
                    ON CONFLICT (ticker, date)
                    DO UPDATE SET
                        article_count = EXCLUDED.article_count,
                        avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                        bullish_count = EXCLUDED.bullish_count,
                        neutral_count = EXCLUDED.neutral_count,
                        bearish_count = EXCLUDED.bearish_count
                """)
            finally:
                connection.unregister("news_batch")

            return df.height

        finally:
            if owns_connection:
                connection.close()


def load_marketaux_news_to_gold(
    df: pl.DataFrame,
    connection=None,
) -> int:
    if df.is_empty():
        return 0

    with DATABASE_WRITE_LOCK:
        owns_connection = connection is None

        if owns_connection:
            connection = get_database_connection()

        try:
            create_marketaux_news_table(connection)

            connection.register("marketaux_news_batch", df)

            try:
                connection.execute("""
                    INSERT INTO marketaux_news_daily
                    SELECT
                        ticker, date, article_count,
                        avg_sentiment_score, avg_match_score,
                        bullish_count, neutral_count, bearish_count
                    FROM marketaux_news_batch
                    ON CONFLICT (ticker, date)
                    DO UPDATE SET
                        article_count = EXCLUDED.article_count,
                        avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                        avg_match_score = EXCLUDED.avg_match_score,
                        bullish_count = EXCLUDED.bullish_count,
                        neutral_count = EXCLUDED.neutral_count,
                        bearish_count = EXCLUDED.bearish_count
                """)
            finally:
                connection.unregister("marketaux_news_batch")

            return df.height

        finally:
            if owns_connection:
                connection.close()


def refresh_market_features_to_gold(
    connection=None,
    storage: StorageBackend | None = None,
) -> int:
    """
    Refresh the combined Gold table and optionally export it to storage.

    Returns the total number of records in market_features_daily.
    """

    with DATABASE_WRITE_LOCK:
        owns_connection = connection is None

        if owns_connection:
            connection = get_database_connection()

        try:
            create_stocks_table(connection)
            create_marketaux_news_table(connection)
            create_market_features_table(connection)

            connection.execute("""
                INSERT INTO market_features_daily (
                    ticker, date, open, high, low, close, volume,
                    daily_change_pct, rsi_14, sma_50, sma_200,
                    article_count, avg_sentiment_score, avg_match_score,
                    bullish_count, neutral_count, bearish_count
                )
                SELECT
                    UPPER(s.asset_name) AS ticker,
                    CAST(s.date AS DATE) AS date,
                    s.open,
                    s.high,
                    s.low,
                    s.close,
                    s.volume,
                    CASE
                        WHEN s.previous_close IS NULL
                            OR s.previous_close = 0
                        THEN NULL
                        ELSE ((s.close / s.previous_close) - 1) * 100
                    END AS daily_change_pct,
                    s.rsi_14,
                    s.sma_50,
                    s.sma_200,
                    COALESCE(n.article_count, 0),
                    n.avg_sentiment_score,
                    n.avg_match_score,
                    COALESCE(n.bullish_count, 0),
                    COALESCE(n.neutral_count, 0),
                    COALESCE(n.bearish_count, 0)
                FROM (
                    SELECT
                        *,
                        LAG(close) OVER (
                            PARTITION BY UPPER(asset_name)
                            ORDER BY date
                        ) AS previous_close
                    FROM stocks_daily
                ) s
                LEFT JOIN marketaux_news_daily n
                    ON UPPER(s.asset_name) = UPPER(n.ticker)
                    AND CAST(s.date AS DATE) = n.date
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    daily_change_pct = EXCLUDED.daily_change_pct,
                    rsi_14 = EXCLUDED.rsi_14,
                    sma_50 = EXCLUDED.sma_50,
                    sma_200 = EXCLUDED.sma_200,
                    article_count = EXCLUDED.article_count,
                    avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                    avg_match_score = EXCLUDED.avg_match_score,
                    bullish_count = EXCLUDED.bullish_count,
                    neutral_count = EXCLUDED.neutral_count,
                    bearish_count = EXCLUDED.bearish_count
            """)

            result = connection.execute("""
                SELECT COUNT(*)
                FROM market_features_daily
            """).fetchone()

            if result is None:
                raise RuntimeError(
                    "Failed to retrieve market_features_daily row count."
                )

            rows = int(result[0])

            if storage is not None:
                arrow_table = connection.execute("""
                    SELECT *
                    FROM market_features_daily
                    ORDER BY ticker, date
                """).to_arrow_table()

                gold_data = pl.from_arrow(arrow_table)

                if isinstance(gold_data, pl.Series):
                    gold_data = gold_data.to_frame()

                storage.write_parquet(
                    MARKET_FEATURES_GOLD_KEY,
                    gold_data,
                    overwrite=True,
                )

            return rows

        finally:
            if owns_connection:
                connection.close()
