
import duckdb
import pytest

from market_data.database.connection import get_database_connection
from market_data.database.tables import (
    create_stocks_table,
    create_news_table,
    create_marketaux_news_table,
    create_market_features_table,
)


def setup_database(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(db_connection, "DATABASE_DIR", database_dir)
    monkeypatch.setattr(db_connection, "DATABASE_PATH", database_path)

    return get_database_connection()


def get_columns(connection, table_name):
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()

    return [row[0] for row in rows]


def test_create_stocks_table(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_stocks_table(conn)

        assert get_columns(conn, "stocks_daily") == [
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
        ]
    finally:
        conn.close()


def test_create_stocks_table_is_idempotent(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_stocks_table(conn)
        create_stocks_table(conn)

        assert len(get_columns(conn, "stocks_daily")) == 14
    finally:
        conn.close()


def test_create_stocks_table_primary_key(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_stocks_table(conn)

        insert_sql = """
            INSERT INTO stocks_daily (
                asset_name, date, year, month, day, day_of_week,
                open, high, low, close, volume
            )
            VALUES (
                'NVDA', '2026-11-06', 2026, 11, 6, 5,
                100, 105, 95, 103, 1000
            )
        """

        conn.execute(insert_sql)

        with pytest.raises(Exception):
            conn.execute(insert_sql)
    finally:
        conn.close()


def test_create_stocks_table_migrates_existing_table(tmp_path):
    connection = duckdb.connect(str(tmp_path / "legacy.duckdb"))

    try:
        connection.execute("""
            CREATE TABLE stocks_daily (
                asset_name VARCHAR NOT NULL,
                date TIMESTAMP NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                PRIMARY KEY (asset_name, date)
            )
        """)

        connection.execute("""
            INSERT INTO stocks_daily VALUES (
                'NVDA', '2026-11-06', 2026, 11, 6, 5,
                100, 105, 95, 103, 1000
            )
        """)

        create_stocks_table(connection)

        assert get_columns(connection, "stocks_daily") == [
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
        ]

        existing_row = connection.execute("""
            SELECT asset_name, close, rsi_14, sma_50, sma_200
            FROM stocks_daily
        """).fetchone()

        assert existing_row == ("NVDA", 103.0, None, None, None)
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("create_function", "table_name", "expected_columns"),
    [
        (
            create_news_table,
            "news_daily",
            [
                "ticker",
                "date",
                "article_count",
                "avg_sentiment_score",
                "bullish_count",
                "neutral_count",
                "bearish_count",
            ],
        ),
        (
            create_marketaux_news_table,
            "marketaux_news_daily",
            [
                "ticker",
                "date",
                "article_count",
                "avg_sentiment_score",
                "avg_match_score",
                "bullish_count",
                "neutral_count",
                "bearish_count",
            ],
        ),
        (
            create_market_features_table,
            "market_features_daily",
            [
                "ticker",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "daily_change_pct",
                "rsi_14",
                "sma_50",
                "sma_200",
                "article_count",
                "avg_sentiment_score",
                "avg_match_score",
                "bullish_count",
                "neutral_count",
                "bearish_count",
            ],
        ),
    ],
)
def test_create_other_tables(
    tmp_path,
    monkeypatch,
    create_function,
    table_name,
    expected_columns,
):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_function(conn)

        assert get_columns(conn, table_name) == expected_columns
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("create_function", "table_name"),
    [
        (create_news_table, "news_daily"),
        (create_marketaux_news_table, "marketaux_news_daily"),
        (create_market_features_table, "market_features_daily"),
    ],
)
def test_other_tables_are_idempotent(
    tmp_path,
    monkeypatch,
    create_function,
    table_name,
):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_function(conn)
        create_function(conn)

        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]

        assert count == 1
    finally:
        conn.close()


def test_table_creators_leave_caller_connection_open(tmp_path, monkeypatch):
    conn = setup_database(tmp_path, monkeypatch)

    try:
        create_stocks_table(conn)
        create_news_table(conn)
        create_marketaux_news_table(conn)
        create_market_features_table(conn)

        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()
