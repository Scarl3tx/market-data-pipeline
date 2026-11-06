from alphavantage_trackers.pipelines.gold.database import (
    get_database_connection
)

from alphavantage_trackers.pipelines.av_stocks.gold.gold_alphavantage_stocks import (
    load_stocks_to_gold
)

from alphavantage_trackers.pipelines.av_commodities.gold.gold_alphavantage_commodities import (
    load_commodities_to_gold
)

from alphavantage_trackers.logging_config import get_pipeline_logger
logger = get_pipeline_logger("gold-pipeline")

def run_gold_pipeline():

    logger.info("Starting Gold pipeline.")

    conn = get_database_connection()

    try:

        stocks_success = load_stocks_to_gold(conn)
        commodities_success = load_commodities_to_gold(conn)
        log_gold_table_counts(conn, logger)
        success = stocks_success and commodities_success

        if success:
            logger.info("Gold pipeline completed successfully.")
        else:
            logger.error("Gold pipeline completed with errors.")

        return success

    except Exception:
        logger.exception("Gold pipeline failed.")
        return False

    finally:
        conn.close()

def log_gold_table_counts(conn, logger):
    tables = conn.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
        ORDER BY table_schema, table_name
        """
    ).fetchall()

    logger.info("========== GOLD TABLE SUMMARY ==========")

    for schema_name, table_name in tables:
        row_count = conn.execute(
            f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
        ).fetchone()[0]

        logger.info(
            "%s.%s: %s rows",
            schema_name,
            table_name,
            f"{row_count:,}"
        )

    logger.info("========================================")

if __name__ == "__main__":
    run_gold_pipeline()