from datetime import date

import duckdb
import polars as pl

from market_data.database.tables import create_news_table
from market_data.database.loader import load_news_to_gold


def sample_gold():
    return pl.DataFrame({
        "ticker": ["NVDA"],
        "date": [date(2026, 11, 6)],
        "article_count": [4],
        "avg_sentiment_score": [0.25],
        "bullish_count": [2],
        "neutral_count": [1],
        "bearish_count": [1],
    })

def test_create_news_table(tmp_path):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))

    create_news_table(connection)

    result = connection.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'news_daily'
        ORDER BY ordinal_position
    """).fetchall()

    assert [row[0] for row in result] == [
        "ticker",
        "date",
        "article_count",
        "avg_sentiment_score",
        "bullish_count",
        "neutral_count",
        "bearish_count",
    ]

    connection.close()


def test_create_news_table_is_idempotent(tmp_path):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))

    create_news_table(connection)
    create_news_table(connection)

    result = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'news_daily'"
    ).fetchone()[0]

    assert result == 1
    connection.close()


def test_load_news_to_gold_success(tmp_path):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))

    rows_loaded = load_news_to_gold(sample_gold(), connection)

    result = connection.execute(
        "SELECT * FROM news_daily"
    ).fetchall()

    assert rows_loaded == 1
    assert len(result) == 1
    assert result[0][0] == "NVDA"
    assert result[0][2] == 4

    connection.close()


def test_load_news_to_gold_updates_existing_record(tmp_path):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))

    load_news_to_gold(sample_gold(), connection)

    updated = sample_gold().with_columns(
        pl.lit(6, dtype=pl.UInt32).alias("article_count"),
        pl.lit(0.5).alias("avg_sentiment_score"),
        pl.lit(4, dtype=pl.UInt32).alias("bullish_count"),
    )

    load_news_to_gold(updated, connection)

    result = connection.execute("""
        SELECT COUNT(*), article_count, avg_sentiment_score
        FROM news_daily
        GROUP BY article_count, avg_sentiment_score
    """).fetchone()

    assert result == (1, 6, 0.5)

    connection.close()


def test_load_news_to_gold_empty_dataframe(tmp_path):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))

    result = load_news_to_gold(sample_gold().head(0), connection)

    assert result == 0

    connection.close()