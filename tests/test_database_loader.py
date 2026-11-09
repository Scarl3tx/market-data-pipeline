
from datetime import datetime
import pytest
import polars as pl

from market_data.database.connection import get_database_connection
from market_data.database.loader import (
    load_stocks_to_gold,
    load_news_to_gold,
    load_marketaux_news_to_gold,
    refresh_market_features_to_gold,
)


def make_silver_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "year": [2026, 2026],
        "month": [11, 11],
        "day": [5, 6],
        "day_of_week": [4, 5],
        "open": [100.0, 102.0],
        "high": [105.0, 107.0],
        "low": [98.0, 100.0],
        "close": [103.0, 106.0],
        "volume": [1000, 1200],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def make_indicator_df():
    return make_silver_df().with_columns([
        pl.Series("rsi_14", [55.0, 60.0], dtype=pl.Float64),
        pl.Series("sma_50", [100.0, 101.0], dtype=pl.Float64),
        pl.Series("sma_200", [95.0, 96.0], dtype=pl.Float64),
    ])


def setup_database(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(db_connection, "DATABASE_DIR", database_dir)
    monkeypatch.setattr(db_connection, "DATABASE_PATH", database_path)

    return get_database_connection()


def test_load_stocks_to_gold(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = make_silver_df()
    result = load_stocks_to_gold(df, conn)

    assert result == 2

    rows = conn.execute("""
        SELECT COUNT(*) FROM stocks_daily
    """).fetchone()

    assert rows[0] == 2
    conn.close()


def test_load_stocks_to_gold_upserts_existing_records(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = make_silver_df()
    load_stocks_to_gold(df, conn)

    updated_df = df.with_columns(
        pl.when(pl.col("date") == datetime(2026, 11, 6))
        .then(pl.lit(110.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )

    load_stocks_to_gold(updated_df, conn)

    rows = conn.execute("""
        SELECT COUNT(*) FROM stocks_daily
    """).fetchone()

    updated_close = conn.execute("""
        SELECT close
        FROM stocks_daily
        WHERE asset_name = 'NVDA'
        AND date = '2026-11-06'
    """).fetchone()

    assert rows[0] == 2
    assert updated_close[0] == 110.0
    conn.close()


def test_load_stocks_to_gold_empty_dataframe():
    result = load_stocks_to_gold(pl.DataFrame())
    assert result == 0


def test_load_stocks_to_gold_saves_indicators(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    result = load_stocks_to_gold(make_indicator_df(), conn)
    assert result == 2

    rows = conn.execute("""
        SELECT rsi_14, sma_50, sma_200
        FROM stocks_daily
        ORDER BY date
    """).fetchall()

    assert rows == [
        (55.0, 100.0, 95.0),
        (60.0, 101.0, 96.0),
    ]
    conn.close()


def test_load_stocks_to_gold_preserves_null_indicators(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = make_silver_df().with_columns([
        pl.Series("rsi_14", [None, None], dtype=pl.Float64),
        pl.Series("sma_50", [None, None], dtype=pl.Float64),
        pl.Series("sma_200", [None, None], dtype=pl.Float64),
    ])

    load_stocks_to_gold(df, conn)

    rows = conn.execute("""
        SELECT
            rsi_14 IS NULL,
            sma_50 IS NULL,
            sma_200 IS NULL
        FROM stocks_daily
        ORDER BY date
    """).fetchall()

    assert rows == [
        (True, True, True),
        (True, True, True),
    ]
    conn.close()


def test_load_stocks_to_gold_supports_missing_indicator_columns(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    load_stocks_to_gold(make_silver_df(), conn)

    rows = conn.execute("""
        SELECT rsi_14, sma_50, sma_200
        FROM stocks_daily
    """).fetchall()

    assert rows == [
        (None, None, None),
        (None, None, None),
    ]
    conn.close()


def test_load_news_to_gold(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    result = load_news_to_gold(df, conn)
    assert result == 1

    row = conn.execute("""
        SELECT *
        FROM news_daily
        WHERE ticker = 'NVDA'
    """).fetchone()

    assert row == (
        "NVDA",
        datetime(2026, 11, 6).date(),
        10,
        0.65,
        7,
        2,
        1,
    )
    conn.close()


def test_load_news_to_gold_upserts_existing_records(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    load_news_to_gold(df, conn)

    updated_df = df.with_columns(
        pl.lit(20).alias("article_count"),
        pl.lit(0.8).alias("avg_sentiment_score"),
    )

    load_news_to_gold(updated_df, conn)

    row = conn.execute("""
        SELECT article_count, avg_sentiment_score
        FROM news_daily
        WHERE ticker = 'NVDA'
    """).fetchone()

    assert row == (20, 0.8)
    conn.close()


def test_load_marketaux_news_to_gold(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "avg_match_score": [0.9],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    result = load_marketaux_news_to_gold(df, conn)
    assert result == 1

    row = conn.execute("""
        SELECT *
        FROM marketaux_news_daily
        WHERE ticker = 'NVDA'
    """).fetchone()

    assert row == (
        "NVDA",
        datetime(2026, 11, 6).date(),
        10,
        0.65,
        0.9,
        7,
        2,
        1,
    )
    conn.close()


def test_load_marketaux_news_to_gold_upserts_existing_records(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "avg_match_score": [0.9],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    load_marketaux_news_to_gold(df, conn)

    updated_df = df.with_columns(
        pl.lit(20).alias("article_count"),
        pl.lit(0.75).alias("avg_sentiment_score"),
        pl.lit(0.95).alias("avg_match_score"),
    )

    load_marketaux_news_to_gold(updated_df, conn)

    row = conn.execute("""
        SELECT
            article_count,
            avg_sentiment_score,
            avg_match_score
        FROM marketaux_news_daily
        WHERE ticker = 'NVDA'
    """).fetchone()

    assert row == (20, 0.75, 0.95)
    conn.close()


def test_refresh_market_features_to_gold(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    load_stocks_to_gold(make_indicator_df(), conn)

    news_df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "avg_match_score": [0.9],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    load_marketaux_news_to_gold(news_df, conn)

    result = refresh_market_features_to_gold(conn)
    assert result == 2

    rows = conn.execute("""
        SELECT
            ticker,
            date,
            daily_change_pct,
            article_count,
            avg_sentiment_score,
            avg_match_score,
            bullish_count,
            neutral_count,
            bearish_count
        FROM market_features_daily
        ORDER BY date
    """).fetchall()

    assert rows[0] == (
        "NVDA",
        datetime(2026, 11, 5).date(),
        None,
        0,
        None,
        None,
        0,
        0,
        0,
    )

    assert rows[1][0:2] == (
        "NVDA",
        datetime(2026, 11, 6).date(),
    )

    assert abs(rows[1][2] - ((106.0 / 103.0 - 1) * 100)) < 1e-9

    assert rows[1][3:] == (
        10,
        0.65,
        0.9,
        7,
        2,
        1,
    )
    conn.close()


def test_refresh_market_features_to_gold_is_idempotent(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    load_stocks_to_gold(make_indicator_df(), conn)

    first_result = refresh_market_features_to_gold(conn)
    second_result = refresh_market_features_to_gold(conn)

    count = conn.execute("""
        SELECT COUNT(*) FROM market_features_daily
    """).fetchone()[0]

    assert first_result == 2
    assert second_result == 2
    assert count == 2
    conn.close()

def test_load_stocks_to_gold_keeps_caller_connection_open(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    load_stocks_to_gold(make_silver_df(), conn)

    # The loader must not close a connection it did not create.
    result = conn.execute("SELECT COUNT(*) FROM stocks_daily").fetchone()

    assert result[0] == 2

    conn.close()


def test_load_news_to_gold_keeps_caller_connection_open(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    load_news_to_gold(df, conn)

    assert conn.execute(
        "SELECT COUNT(*) FROM news_daily"
    ).fetchone()[0] == 1

    conn.close()


def test_load_marketaux_news_to_gold_keeps_caller_connection_open(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [10],
        "avg_sentiment_score": [0.65],
        "avg_match_score": [0.9],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    load_marketaux_news_to_gold(df, conn)

    assert conn.execute(
        "SELECT COUNT(*) FROM marketaux_news_daily"
    ).fetchone()[0] == 1

    conn.close()


def test_failed_stock_upsert_unregisters_temporary_relation(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    invalid_df = make_silver_df().with_columns(
        pl.lit(None, dtype=pl.Float64).alias("close")
    )

    with pytest.raises(Exception):
        load_stocks_to_gold(invalid_df, conn)

    # The temporary relation must be removed even after SQL failure.
    with pytest.raises(Exception):
        conn.execute("SELECT * FROM silver_batch")

    # The caller's connection must remain usable.
    conn.execute("SELECT 1")

    conn.close()


def test_failed_news_upsert_unregisters_temporary_relation(
    tmp_path,
    monkeypatch,
):
    conn = setup_database(tmp_path, monkeypatch)

    invalid_df = pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [datetime(2026, 11, 6).date()],
        "article_count": [-1],
        "avg_sentiment_score": [0.65],
        "bullish_count": [7],
        "neutral_count": [2],
        "bearish_count": [1],
    })

    with pytest.raises(Exception):
        load_news_to_gold(invalid_df, conn)

    with pytest.raises(Exception):
        conn.execute("SELECT * FROM news_batch")

    conn.execute("SELECT 1")
    conn.close()