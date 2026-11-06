
import logging
import re
from typing import cast
from alphavantage_trackers.paths import STOCKS_SILVER_PATH
from src.historical.paths import HISTORICAL_SILVER_STOCKS_PATH

logger = logging.getLogger("stocks")
import polars as pl

from alphavantage_trackers.pipelines.gold.transformations import (
    calculate_all_indicators,
)

def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

def _upsert_stock_files(
    conn,
    parquet_files,
    source_priority: str
) -> bool:
    """
    Load stock Silver files into Gold.

    Historical:
        Only fills missing values in existing Gold records.

    Alpha Vantage:
        Updates Gold with valid incoming values.
    """

    if source_priority not in ("historical", "alphavantage"):
        raise ValueError(f"Invalid source priority: {source_priority}")

    success = True

    for parquet_file in parquet_files:

        asset_name = parquet_file.stem.removesuffix("_silver")

        if not re.fullmatch(r"[A-Za-z0-9_]+", asset_name):
            logger.error("Invalid stocks table name: %s", asset_name)
            success = False
            continue

        table_name = quote_identifier(asset_name)
        parquet_path = str(parquet_file)

        try:
            # Get incoming Silver columns
            source_columns = [
                column[0]
                for column in conn.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0",
                    [parquet_path]
                ).description
            ]

            if "date" not in source_columns:
                raise ValueError(
                    f"Missing required merge key 'date' for {asset_name}"
                )

            # Check whether the Gold table exists
            table_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'stocks'
                  AND table_name = ?
                """,
                [asset_name]
            ).fetchone()[0] > 0

            # First load: create Gold table using incoming schema
            if not table_exists:
                conn.execute(
                    f"""
                    CREATE TABLE stocks.{table_name} AS
                    SELECT *
                    FROM read_parquet(?)
                    """,
                    [parquet_path]
                )

                logger.info(
                    "Created Gold table for stocks %s using %s data.",
                    asset_name,
                    source_priority
                )
                continue

            # Get existing Gold columns
            target_columns = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'stocks'
                      AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [asset_name]
                ).fetchall()
            ]

            # Only operate on columns present in both datasets
            common_columns = [
                column
                for column in source_columns
                if column in target_columns
            ]

            if "date" not in common_columns:
                raise ValueError(
                    f"Missing common merge key 'date' for {asset_name}"
                )

            update_columns = [
                column
                for column in common_columns
                if column != "date"
            ]

            # Build source-value validity condition
            update_assignments = []

            for column in update_columns:

                quoted_column = quote_identifier(column)

                source_has_value = (
                    f"NULLIF("
                    f"TRIM(CAST(source.{quoted_column} AS VARCHAR)), "
                    f"'') IS NOT NULL"
                )

                if source_priority == "historical":

                    target_is_missing = (
                        f"NULLIF("
                        f"TRIM(CAST(target.{quoted_column} AS VARCHAR)), "
                        f"'') IS NULL"
                    )

                    assignment = (
                        f"{quoted_column} = CASE "
                        f"WHEN {target_is_missing} "
                        f"AND {source_has_value} "
                        f"THEN source.{quoted_column} "
                        f"ELSE target.{quoted_column} END"
                    )

                else:

                    assignment = (
                        f"{quoted_column} = CASE "
                        f"WHEN {source_has_value} "
                        f"THEN source.{quoted_column} "
                        f"ELSE target.{quoted_column} END"
                    )

                update_assignments.append(assignment)

            update_clause = ", ".join(update_assignments)

            insert_columns = ", ".join(
                quote_identifier(column)
                for column in target_columns
            )

            # Include all Gold columns when inserting.
            # Columns absent from the incoming source become NULL.
            insert_values = ", ".join(
                (
                    f"source.{quote_identifier(column)}"
                    if column in source_columns
                    else "NULL"
                )
                for column in target_columns
            )

            merge_sql = f"""
                MERGE INTO stocks.{table_name} AS target

                USING read_parquet(?) AS source

                ON target."date" = source."date"

                WHEN MATCHED THEN
                    UPDATE SET {update_clause}

                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            conn.execute(merge_sql, [parquet_path])

            logger.info(
                "Merged stocks %s using %s data.",
                asset_name,
                source_priority
            )

        except Exception:
            logger.exception(
                "Failed to load stocks %s into Gold using %s data.",
                asset_name,
                source_priority
            )
            success = False

    return success

def recalculate_stock_indicators(conn) -> bool:
    """
    Recalculate technical indicators for all consolidated Gold stock tables.

    Reads the complete Gold dataset, calculates indicators using Polars,
    and updates the existing DuckDB tables.
    """

    indicator_columns = [
        "pct_change",
        "sma_7",
        "sma_30",
        "volatility_7",
        "volatility_30",
        "rsi_7",
        "rsi_14",
        "rsi_21",
    ]

    stock_tables = [
        row[0]
        for row in conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'stocks'
              AND table_type = 'BASE TABLE'
        """).fetchall()
    ]

    success = True

    for asset_name in stock_tables:

        if not re.fullmatch(r"[A-Za-z0-9_]+", asset_name):
            logger.error("Invalid stocks table name: %s", asset_name)
            success = False
            continue

        table_name = quote_identifier(asset_name)

        try:
            logger.info(
                "Calculating technical indicators for %s...",
                asset_name
            )

            # Ensure all calculated columns exist in Gold.
            existing_columns = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'stocks'
                      AND table_name = ?
                    """,
                    [asset_name]
                ).fetchall()
            }

            for column in indicator_columns:
                if column not in existing_columns:
                    conn.execute(
                        f"""
                        ALTER TABLE stocks.{table_name}
                        ADD COLUMN {quote_identifier(column)} DOUBLE
                        """
                    )

            # Read the complete consolidated Gold dataset.
            arrow_table = conn.sql(
                f"""
                SELECT *
                FROM stocks.{table_name}
                ORDER BY date
                """
            ).to_arrow_table()

            df = cast(pl.DataFrame, pl.from_arrow(arrow_table))

            if df.is_empty():
                logger.warning(
                    "Skipping indicator calculations for empty table %s.",
                    asset_name
                )
                continue

            # Calculate indicators across the entire dataset.
            df = calculate_all_indicators(df)

            # Prepare only the columns that need updating.
            updates = df.select(
                ["date"] + indicator_columns
            )

            # Register the calculated results temporarily in DuckDB.
            conn.register(
                "indicator_updates",
                updates.to_arrow()
            )

            try:
                assignments = ", ".join(
                    f"{quote_identifier(column)} = "
                    f"source.{quote_identifier(column)}"
                    for column in indicator_columns
                )

                conn.execute(
                    f"""
                    UPDATE stocks.{table_name} AS target

                    SET {assignments}

                    FROM indicator_updates AS source

                    WHERE target."date" = source."date"
                    """
                )

            finally:
                conn.unregister("indicator_updates")

            logger.info(
                "Successfully recalculated indicators for %s (%s rows).",
                asset_name,
                df.height
            )

        except Exception:
            logger.exception(
                "Failed to calculate indicators for %s.",
                asset_name
            )
            success = False

    return success

def load_stocks_to_gold(conn) -> bool:
    """
    Load historical data first, followed by Alpha Vantage data.

    Alpha Vantage takes precedence over historical data.
    """

    conn.execute("CREATE SCHEMA IF NOT EXISTS stocks")

    historical_files = list(
        HISTORICAL_SILVER_STOCKS_PATH.glob("*_silver.pq")
    )

    alphavantage_files = list(
        STOCKS_SILVER_PATH.glob("*/*_silver.pq")
    )

    logger.info(
        "Found %s historical stock Silver files.",
        len(historical_files)
    )

    logger.info(
        "Found %s Alpha Vantage stock Silver files.",
        len(alphavantage_files)
    )

    # Ensure Alpha Vantage creates the full Gold schema first
    # whenever its Silver dataset exists.
    historical_success = True
    alphavantage_success = True

    if alphavantage_files:
        alphavantage_success = _upsert_stock_files(
            conn=conn,
            parquet_files=alphavantage_files,
            source_priority="alphavantage"
        )

        # Historical data is merged afterwards.
        # It cannot overwrite valid Alpha Vantage values.
        historical_success = _upsert_stock_files(
            conn=conn,
            parquet_files=historical_files,
            source_priority="historical"
        )

    else:
        # Fallback when no Alpha Vantage Silver data exists yet.
        historical_success = _upsert_stock_files(
            conn=conn,
            parquet_files=historical_files,
            source_priority="historical"
        )

    if not (historical_success and alphavantage_success):
        return False

    indicator_success = recalculate_stock_indicators(conn)

    return indicator_success
