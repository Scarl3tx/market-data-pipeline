from datetime import datetime

import polars as pl

from market_data.pipelines.twelvedata.bronze.storage import (
    save_stock_batch,
    get_stock_output_path
)
from market_data.pipelines.twelvedata.silver.stocks import (
    run_stock_silver
)
from market_data.pipelines.twelvedata.gold.stocks import (
    run_stock_gold
)
from market_data.database.connection import get_database_connection


def make_bronze_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 4),
            datetime(2026, 11, 5),
            datetime(2026, 11, 6)
        ],
        "open": [100.0, 102.0, 104.0],
        "high": [105.0, 107.0, 109.0],
        "low": [98.0, 100.0, 102.0],
        "close": [103.0, 106.0, 108.0],
        "volume": [1000, 1200, 1500]
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_bronze_to_silver_to_gold(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(
        db_connection,
        "DATABASE_DIR",
        database_dir
    )
    monkeypatch.setattr(
        db_connection,
        "DATABASE_PATH",
        database_path
    )

    # 1. Save Bronze data
    bronze_df = make_bronze_df()

    bronze_output = get_stock_output_path(
        bronze_path,
        "NVDA",
        "20261104",
        "20261106"
    )

    save_stock_batch(bronze_df, bronze_output)

    # 2. Run Silver pipeline
    silver_result = run_stock_silver(
        symbol="NVDA",
        bronze_path=bronze_path,
        silver_path=silver_path
    )

    assert silver_result["success"] is True
    assert silver_result["rows"] == 3

    # 3. Run Gold pipeline
    gold_result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path
    )
    print("GOLD RESULT:", gold_result)
    assert gold_result["success"] is True
    print("SILVER RESULT:", silver_result)
    assert silver_result.get("rows") == 3

    # 4. Verify DuckDB contents
    conn = get_database_connection()

    result = conn.execute("""
        SELECT
            asset_name,
            COUNT(*) AS rows,
            MIN(date) AS earliest_date,
            MAX(date) AS latest_date
        FROM stocks_daily
        GROUP BY asset_name
    """).fetchone()

    conn.close()

    assert result[0] == "NVDA"
    assert result[1] == 3
    assert result[2].date().isoformat() == "2026-11-04"
    assert result[3].date().isoformat() == "2026-11-06"

def test_repeated_gold_runs_do_not_duplicate_records(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(
        db_connection,
        "DATABASE_DIR",
        database_dir
    )
    monkeypatch.setattr(
        db_connection,
        "DATABASE_PATH",
        database_path
    )

    # Save Bronze data
    bronze_df = make_bronze_df()

    bronze_output = get_stock_output_path(
        bronze_path,
        "NVDA",
        "20261104",
        "20261106"
    )

    save_stock_batch(bronze_df, bronze_output)

    # Generate Silver data
    silver_result = run_stock_silver(
        symbol="NVDA",
        bronze_path=bronze_path,
        silver_path=silver_path
    )

    assert silver_result["success"] is True

    # Run Gold twice
    first_result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path
    )

    second_result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path
    )

    assert first_result["success"] is True
    assert second_result["success"] is True

    conn = get_database_connection()

    count = conn.execute(
        "SELECT COUNT(*) FROM stocks_daily"
    ).fetchone()[0]

    conn.close()

    assert count == 3