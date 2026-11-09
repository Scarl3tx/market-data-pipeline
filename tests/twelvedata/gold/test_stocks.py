
from datetime import datetime

import polars as pl

from market_data.database.connection import get_database_connection
from market_data.pipelines.twelvedata.gold.stocks import (
    run_stock_gold,
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


def make_invalid_row():
    return pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 7)],
        "year": [2026],
        "month": [11],
        "day": [7],
        "day_of_week": [6],
        "open": [-1.0],
        "high": [107.0],
        "low": [100.0],
        "close": [106.0],
        "volume": [-100],
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_run_stock_gold_success(tmp_path, monkeypatch):
    import market_data.database.connection as db_connection

    database_dir = tmp_path / "database"
    database_path = database_dir / "test.duckdb"

    monkeypatch.setattr(
        db_connection,
        "DATABASE_DIR",
        database_dir,
    )
    monkeypatch.setattr(
        db_connection,
        "DATABASE_PATH",
        database_path,
    )

    silver_path = tmp_path / "silver"
    stock_path = silver_path / "NVDA"
    stock_path.mkdir(parents=True)

    make_silver_df().write_parquet(
        stock_path / "NVDA_silver.pq"
    )

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path,
    )

    assert result["success"] is True
    assert result["rows"] == 2
    assert result["rejected_rows"] == 0
    assert result["quarantined_rows"] == 0

    conn = get_database_connection()

    count = conn.execute(
        "SELECT COUNT(*) FROM stocks_daily"
    ).fetchone()[0]

    conn.close()

    assert count == 2


def test_run_stock_gold_skips_missing_data(tmp_path):
    result = run_stock_gold(
        symbol="NVDA",
        silver_path=tmp_path / "missing",
    )

    assert result["success"] is True
    assert result["skipped"] is True


def test_run_stock_gold_handles_empty_folder(tmp_path):
    stock_path = tmp_path / "silver" / "NVDA"
    stock_path.mkdir(parents=True)

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=tmp_path / "silver",
    )

    assert result["success"] is True
    assert result["skipped"] is True


def test_run_stock_gold_handles_database_failure(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import stocks

    def raise_error(**kwargs):
        raise RuntimeError("Database failure")

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        raise_error,
    )

    stock_path = tmp_path / "silver" / "NVDA"
    stock_path.mkdir(parents=True)

    make_silver_df().write_parquet(
        stock_path / "NVDA_silver.pq"
    )

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=tmp_path / "silver",
    )

    assert result["success"] is False
    assert "Database failure" in result["error"]


def test_run_stock_gold_loads_only_valid_rows(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import stocks

    silver_path = tmp_path / "silver"
    stock_path = silver_path / "NVDA"
    stock_path.mkdir(parents=True)

    pl.concat([
        make_silver_df(),
        make_invalid_row(),
    ]).write_parquet(
        stock_path / "NVDA_silver.pq"
    )

    captured = {}

    def fake_loader(df, connection=None):
        captured["df"] = df
        return df.height

    def fake_quarantine(symbol, rejected_df):
        captured["rejected_df"] = rejected_df
        return rejected_df.height

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        fake_loader,
    )

    monkeypatch.setattr(
        stocks,
        "save_rejected_stock_data",
        fake_quarantine,
    )

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path,
    )

    assert result["success"] is True
    assert result["rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["quarantined_rows"] == 1

    assert captured["df"].height == 2
    assert captured["rejected_df"].height == 1

    assert result["validation_reasons"]["invalid_open"] == 1
    assert result["validation_reasons"]["invalid_volume"] == 1


def test_run_stock_gold_handles_all_invalid_rows(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import stocks

    silver_path = tmp_path / "silver"
    stock_path = silver_path / "NVDA"
    stock_path.mkdir(parents=True)

    invalid_df = make_silver_df().with_columns(
        pl.lit(-1.0).alias("open")
    )

    invalid_df.write_parquet(
        stock_path / "NVDA_silver.pq"
    )

    captured = {}

    def loader_should_not_run(**kwargs):
        raise AssertionError(
            "Database loader should not run when all rows are invalid"
        )

    def fake_quarantine(symbol, rejected_df):
        captured["rejected_df"] = rejected_df
        return rejected_df.height

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        loader_should_not_run,
    )

    monkeypatch.setattr(
        stocks,
        "save_rejected_stock_data",
        fake_quarantine,
    )

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path,
    )

    assert result["success"] is True
    assert result["rows"] == 0
    assert result["rejected_rows"] == 2
    assert result["quarantined_rows"] == 2
    assert captured["rejected_df"].height == 2


def test_quarantine_failure_does_not_prevent_gold_loading(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import stocks

    silver_path = tmp_path / "silver"
    stock_path = silver_path / "NVDA"
    stock_path.mkdir(parents=True)

    pl.concat([
        make_silver_df(),
        make_invalid_row(),
    ]).write_parquet(
        stock_path / "NVDA_silver.pq"
    )

    loaded = {}

    def fake_loader(df, connection=None):
        loaded["rows"] = df.height
        return df.height

    def failing_quarantine(symbol, rejected_df):
        raise OSError("Disk unavailable")

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        fake_loader,
    )

    monkeypatch.setattr(
        stocks,
        "save_rejected_stock_data",
        failing_quarantine,
    )

    result = run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path,
    )

    assert result["success"] is True
    assert result["rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["quarantined_rows"] == 0
    assert "Disk unavailable" in result["quarantine_error"]
    assert loaded["rows"] == 2


def test_run_stock_gold_does_not_modify_silver_files(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import stocks

    silver_path = tmp_path / "silver"
    stock_path = silver_path / "NVDA"
    stock_path.mkdir(parents=True)

    silver_file = stock_path / "NVDA_silver.pq"

    original_df = pl.concat([
        make_silver_df(),
        make_invalid_row(),
    ])

    original_df.write_parquet(silver_file)

    before = pl.read_parquet(silver_file)

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        lambda df, connection=None: df.height,
    )

    monkeypatch.setattr(
        stocks,
        "save_rejected_stock_data",
        lambda symbol, rejected_df: rejected_df.height,
    )

    run_stock_gold(
        symbol="NVDA",
        silver_path=silver_path,
    )

    after = pl.read_parquet(silver_file)

    assert before.equals(after)
