
from market_data.database.connection import get_database_connection


def create_stocks_table(connection=None):
    owns_connection = connection is None

    if owns_connection:
        connection = get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS stocks_daily (
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

        # Migrate existing databases without deleting historical records.
        connection.execute("""
            ALTER TABLE stocks_daily
            ADD COLUMN IF NOT EXISTS rsi_14 DOUBLE
        """)

        connection.execute("""
            ALTER TABLE stocks_daily
            ADD COLUMN IF NOT EXISTS sma_50 DOUBLE
        """)

        connection.execute("""
            ALTER TABLE stocks_daily
            ADD COLUMN IF NOT EXISTS sma_200 DOUBLE
        """)

    finally:
        if owns_connection:
            connection.close()


def create_news_table(connection=None):
    owns_connection = connection is None

    if owns_connection:
        connection = get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS news_daily (
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                article_count UINTEGER NOT NULL,
                avg_sentiment_score DOUBLE NOT NULL,
                bullish_count UINTEGER NOT NULL,
                neutral_count UINTEGER NOT NULL,
                bearish_count UINTEGER NOT NULL,
                PRIMARY KEY (ticker, date)
            )
        """)

    finally:
        if owns_connection:
            connection.close()


def create_marketaux_news_table(connection=None):
    owns_connection = connection is None

    if owns_connection:
        connection = get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marketaux_news_daily (
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                article_count UINTEGER NOT NULL,
                avg_sentiment_score DOUBLE,
                avg_match_score DOUBLE,
                bullish_count UINTEGER NOT NULL,
                neutral_count UINTEGER NOT NULL,
                bearish_count UINTEGER NOT NULL,
                PRIMARY KEY (ticker, date)
            )
        """)

    finally:
        if owns_connection:
            connection.close()


def create_market_features_table(connection=None):
    owns_connection = connection is None

    if owns_connection:
        connection = get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS market_features_daily (
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,

                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,

                daily_change_pct DOUBLE,

                rsi_14 DOUBLE,
                sma_50 DOUBLE,
                sma_200 DOUBLE,

                article_count UINTEGER NOT NULL DEFAULT 0,
                avg_sentiment_score DOUBLE,
                avg_match_score DOUBLE,

                bullish_count UINTEGER NOT NULL DEFAULT 0,
                neutral_count UINTEGER NOT NULL DEFAULT 0,
                bearish_count UINTEGER NOT NULL DEFAULT 0,

                PRIMARY KEY (ticker, date)
            )
        """)

    finally:
        if owns_connection:
            connection.close()


def create_pipeline_runs_table(connection=None):
    """Create the persistent pipeline execution history table."""

    owns_connection = connection is None

    if owns_connection:
        connection = get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL,
                duration_seconds DOUBLE NOT NULL,
                status VARCHAR NOT NULL,

                twelvedata_status VARCHAR NOT NULL,
                marketaux_status VARCHAR NOT NULL,
                combined_gold_status VARCHAR NOT NULL,

                twelvedata_stages_json VARCHAR NOT NULL,
                marketaux_stages_json VARCHAR NOT NULL,

                error_summary VARCHAR,
                results_json VARCHAR NOT NULL,

                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

    finally:
        if owns_connection:
            connection.close()
