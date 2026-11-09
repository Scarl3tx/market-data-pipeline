
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl

from market_data.database.connection import get_database_connection
from market_data.database.loader import load_news_to_gold


def test_concurrent_news_writes_preserve_all_records(
    tmp_path,
    monkeypatch,
):
    import market_data.database.connection as db_connection

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(db_connection, "DATABASE_DIR", database_dir)
    monkeypatch.setattr(db_connection, "DATABASE_PATH", database_path)

    def write_batch(batch_number):
        df = pl.DataFrame({
            "ticker": [f"STOCK_{batch_number}"],
            "date": [date(2026, 11, 6)],
            "article_count": [10],
            "avg_sentiment_score": [0.5],
            "bullish_count": [5],
            "neutral_count": [3],
            "bearish_count": [2],
        })

        return load_news_to_gold(df)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(write_batch, range(20)))

    assert results == [1] * 20

    connection = get_database_connection()

    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM news_daily"
        ).fetchone()[0]

        assert count == 20
    finally:
        connection.close()


def test_concurrent_upserts_do_not_create_duplicates(
    tmp_path,
    monkeypatch,
):
    import market_data.database.connection as db_connection

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(db_connection, "DATABASE_DIR", database_dir)
    monkeypatch.setattr(db_connection, "DATABASE_PATH", database_path)

    def write_same_record(_):
        df = pl.DataFrame({
            "ticker": ["NVDA"],
            "date": [date(2026, 11, 6)],
            "article_count": [10],
            "avg_sentiment_score": [0.5],
            "bullish_count": [5],
            "neutral_count": [3],
            "bearish_count": [2],
        })

        return load_news_to_gold(df)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(write_same_record, range(20)))

    assert results == [1] * 20

    connection = get_database_connection()

    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM news_daily"
        ).fetchone()[0]

        assert count == 1
    finally:
        connection.close()
