from market_data.database.connection import get_database_connection


def run_query(connection, title, query):
    print(f"\n{'=' * 65}")
    print(title)
    print("=" * 65)

    rows = connection.execute(query).fetchall()

    if not rows:
        print("No results.")
        return

    for row in rows:
        print(row)


def main():
    connection = get_database_connection()

    try:
        # 1. Marketaux coverage by ticker.
        run_query(
            connection,
            "MARKETAUX NEWS COVERAGE",
            """
            SELECT
                ticker,
                COUNT(*) AS news_days,
                SUM(article_count) AS total_articles,
                MIN(date) AS first_news_date,
                MAX(date) AS latest_news_date,
                COUNT(avg_sentiment_score) AS days_with_sentiment
            FROM marketaux_news_daily
            GROUP BY ticker
            ORDER BY ticker;
            """,
        )

        # 2. Combined Gold sentiment coverage.
        run_query(
            connection,
            "COMBINED GOLD SENTIMENT COVERAGE",
            """
            SELECT
                ticker,
                COUNT(*) AS stock_days,
                COUNT(avg_sentiment_score) AS days_with_sentiment,
                COUNT(*) FILTER (
                    WHERE article_count > 0
                ) AS days_with_articles,
                SUM(COALESCE(article_count, 0)) AS joined_articles,
                ROUND(
                    100.0 * COUNT(avg_sentiment_score)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS sentiment_coverage_pct
            FROM market_features_daily
            GROUP BY ticker;
            """,
        )

        # 3. Actual date overlap.
        run_query(
            connection,
            "MARKETAUX TO STOCK DATE MATCHES",
            """
            SELECT
                n.ticker,
                COUNT(*) AS news_days,
                COUNT(s.date) AS matched_stock_days,
                SUM(n.article_count) AS total_articles
            FROM marketaux_news_daily n
            LEFT JOIN stocks_daily s
                ON UPPER(n.ticker) = UPPER(s.asset_name)
                AND n.date = CAST(s.date AS DATE)
            GROUP BY n.ticker;
            """,
        )

        # 4. Duplicate stock records.
        run_query(
            connection,
            "DUPLICATE STOCK RECORDS",
            """
            SELECT asset_name, CAST(date AS DATE), COUNT(*) AS records
            FROM stocks_daily
            GROUP BY asset_name, CAST(date AS DATE)
            HAVING COUNT(*) > 1;
            """,
        )

        # 5. Duplicate Marketaux daily records.
        run_query(
            connection,
            "DUPLICATE MARKETAUX DAILY RECORDS",
            """
            SELECT ticker, date, COUNT(*) AS records
            FROM marketaux_news_daily
            GROUP BY ticker, date
            HAVING COUNT(*) > 1;
            """,
        )

        # 6. Duplicate combined Gold records.
        run_query(
            connection,
            "DUPLICATE COMBINED GOLD RECORDS",
            """
            SELECT ticker, date, COUNT(*) AS records
            FROM market_features_daily
            GROUP BY ticker, date
            HAVING COUNT(*) > 1;
            """,
        )

        # 7. Inspect the actual five news records.
        run_query(
            connection,
            "MARKETAUX DAILY RECORDS",
            """
            SELECT *
            FROM marketaux_news_daily
            ORDER BY ticker, date;
            """,
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()