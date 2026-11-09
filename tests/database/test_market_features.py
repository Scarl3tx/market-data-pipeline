import duckdb
import pytest

from market_data.database.loader import (
    refresh_market_features_to_gold,
)
from market_data.database.tables import (
    create_marketaux_news_table,
    create_stocks_table,
)


@pytest.fixture
def connection():
    con = duckdb.connect(":memory:")

    create_stocks_table(con)
    create_marketaux_news_table(con)

    yield con

    con.close()


def test_refresh_combines_stocks_and_marketaux(connection):
    connection.execute("""
        INSERT INTO stocks_daily (
            asset_name, date,
            year, month, day, day_of_week,
            open, high, low, close, volume
        )
        VALUES
            ('NVDA', '2026-11-02', 2026, 11, 2, 1,
             98, 102, 97, 100, 1000),
            ('NVDA', '2026-11-03', 2026, 11, 3, 2,
             100, 111, 99, 110, 1200),
            ('NVDA', '2026-11-04', 2026, 11, 4, 3,
             110, 111, 98, 99, 1500)
    """)

    connection.execute("""
        INSERT INTO marketaux_news_daily (
            ticker, date,
            article_count,
            avg_sentiment_score,
            avg_match_score,
            bullish_count,
            neutral_count,
            bearish_count
        )
        VALUES (
            'NVDA', '2026-11-03',
            4, 0.35, 0.90, 3, 1, 0
        )
    """)

    rows = refresh_market_features_to_gold(connection)

    assert rows == 3

    result = connection.execute("""
        SELECT
            ticker,
            date,
            close,
            daily_change_pct,
            article_count,
            avg_sentiment_score,
            bullish_count
        FROM market_features_daily
        ORDER BY date
    """).fetchall()

    assert result[0][0] == "NVDA"
    assert result[0][3] is None
    assert result[0][4] == 0

    assert result[1][3] == pytest.approx(10.0)
    assert result[1][4] == 4
    assert result[1][5] == pytest.approx(0.35)
    assert result[1][6] == 3

    assert result[2][3] == pytest.approx(-10.0)
    assert result[2][4] == 0


def test_refresh_is_idempotent(connection):
    connection.execute("""
        INSERT INTO stocks_daily (
            asset_name, date,
            year, month, day, day_of_week,
            open, high, low, close, volume
        )
        VALUES
            ('NVDA', '2026-11-02', 2026, 11, 2, 1,
             98, 102, 97, 100, 1000),
            ('NVDA', '2026-11-03', 2026, 11, 3, 2,
             100, 111, 99, 110, 1200)
    """)

    refresh_market_features_to_gold(connection)
    refresh_market_features_to_gold(connection)

    count = connection.execute("""
        SELECT COUNT(*)
        FROM market_features_daily
    """).fetchone()[0]

    assert count == 2