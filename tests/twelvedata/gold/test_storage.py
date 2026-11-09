
from datetime import datetime

import polars as pl

from market_data.storage.local import LocalStorage
from market_data.pipelines.twelvedata.gold.stocks import (
    run_stock_gold,
)
from market_data.pipelines.twelvedata.gold.quarantine import (
    save_rejected_stock_data,
)


def make_stock_df(
    dates=None,
    opens=None,
):
    if dates is None:
        dates = [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ]

    if opens is None:
        opens = [100.0 + (i * 2) for i in range(len(dates))]

    if len(dates) != len(opens):
        raise ValueError("dates and opens must have equal lengths")

    return pl.DataFrame({
        "asset_name": ["NVDA"] * len(dates),
        "date": dates,
        "year": [2026] * len(dates),
        "month": [11] * len(dates),
        "day": [item.day for item in dates],
        "day_of_week": [item.weekday() for item in dates],
        "open": opens,
        "high": [value + 5 for value in opens],
        "low": [value - 2 for value in opens],
        "close": [value + 3 for value in opens],
        "volume": [1000] * len(dates),
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_gold_storage_reads_silver_and_writes_gold(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")

    silver_key = (
        "silver/stocks/NVDA/NVDA_silver.pq"
    )

    storage.write_parquet(
        silver_key,
        make_stock_df(),
    )

    database_calls = []

    def capture_database_load(**kwargs):
        database_calls.append(kwargs)
        return kwargs["df"].height

    import market_data.pipelines.twelvedata.gold.stocks as stocks

    monkeypatch.setattr(
        stocks,
        "load_stocks_to_gold",
        capture_database_load,
    )

    result = run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    assert result["success"] is True
    assert result["rows"] == 2

    # Verify DuckDB loading was requested.
    assert len(database_calls) == 1

    loaded_df = database_calls[0]["df"]

    assert loaded_df.height == 2
    assert loaded_df["asset_name"].to_list() == [
        "NVDA",
        "NVDA",
    ]

    # Verify S3/storage Gold output was also written.
    gold_key = "gold/stocks/NVDA/NVDA_gold.pq"

    assert storage.exists(gold_key)

    gold_df = storage.read_parquet(gold_key)

    assert gold_df.height == 2

    assert {
        "rsi_14",
        "sma_50",
        "sma_200",
    }.issubset(gold_df.columns)


def test_gold_storage_preserves_existing_dates(
    tmp_path,
):
    storage = LocalStorage(tmp_path / "storage")

    gold_key = "gold/stocks/NVDA/NVDA_gold.pq"

    first_batch = make_stock_df(
        dates=[datetime(2026, 11, 5)],
    )

    second_batch = make_stock_df(
        dates=[datetime(2026, 11, 6)],
    )

    storage.write_parquet(
        "silver/stocks/NVDA/NVDA_silver.pq",
        first_batch,
    )

    first_result = run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    assert first_result["success"] is True

    storage.write_parquet(
        "silver/stocks/NVDA/NVDA_silver.pq",
        second_batch,
    )

    second_result = run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    assert second_result["success"] is True

    gold_df = storage.read_parquet(gold_key)

    assert gold_df.height == 2
    assert gold_df["date"].min() == datetime(2026, 11, 5)
    assert gold_df["date"].max() == datetime(2026, 11, 6)


def test_gold_storage_updates_existing_date(
    tmp_path,
):
    storage = LocalStorage(tmp_path / "storage")

    gold_key = "gold/stocks/NVDA/NVDA_gold.pq"
    silver_key = "silver/stocks/NVDA/NVDA_silver.pq"

    original = make_stock_df(
        dates=[datetime(2026, 11, 5)],
        opens=[100.0],
    )

    updated = make_stock_df(
        dates=[datetime(2026, 11, 5)],
        opens=[110.0],
    )

    storage.write_parquet(silver_key, original)

    run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    storage.write_parquet(silver_key, updated)

    run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    gold_df = storage.read_parquet(gold_key)

    assert gold_df.height == 1
    assert gold_df.item(0, "open") == 110.0


def test_gold_storage_skips_missing_silver(
    tmp_path,
):
    storage = LocalStorage(tmp_path / "storage")

    result = run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    assert result["success"] is True
    assert result["skipped"] is True


def test_quarantine_storage_appends_and_deduplicates(
    tmp_path,
):
    storage = LocalStorage(tmp_path / "storage")

    first = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 5)],
        "open": [-1.0],
        "validation_errors": ["invalid_open"],
    })

    second = pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "open": [-2.0, -3.0],
        "validation_errors": [
            "invalid_close",
            "invalid_volume",
        ],
    })

    assert save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=first,
        storage=storage,
    ) == 1

    assert save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=second,
        storage=storage,
    ) == 1

    key = (
        "quarantine/twelvedata/stocks/"
        "NVDA/NVDA_rejected.pq"
    )

    stored = storage.read_parquet(key)

    assert stored.height == 2

    updated_error = stored.filter(
        pl.col("date") == datetime(2026, 11, 5)
    ).item(0, "validation_errors")

    assert updated_error == "invalid_close"


def test_gold_storage_quarantine_failure_does_not_block_gold(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")

    silver_key = (
        "silver/stocks/NVDA/NVDA_silver.pq"
    )

    mixed_df = pl.concat([
        make_stock_df(),
        make_stock_df(
            dates=[datetime(2026, 11, 7)],
            opens=[-1.0],
        ),
    ])

    storage.write_parquet(
        silver_key,
        mixed_df,
    )

    import market_data.pipelines.twelvedata.gold.stocks as stocks

    def failing_quarantine(**kwargs):
        raise OSError("Storage unavailable")

    monkeypatch.setattr(
        stocks,
        "save_rejected_stock_data",
        failing_quarantine,
    )

    result = run_stock_gold(
        symbol="NVDA",
        storage=storage,
    )

    assert result["success"] is True
    assert result["rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["quarantined_rows"] == 0
    assert "Storage unavailable" in result["quarantine_error"]

    assert storage.exists(
        "gold/stocks/NVDA/NVDA_gold.pq"
    )


def test_gold_coordinator_passes_storage_to_stock_pipeline(
    tmp_path,
    monkeypatch,
):
    from market_data.pipelines.twelvedata.gold import coordinator

    storage = LocalStorage(tmp_path / "storage")
    captured = []

    monkeypatch.setattr(
        coordinator,
        "load_stocks_config",
        lambda: [
            {"symbol": "NVDA"},
            {"symbol": "AAPL"},
        ],
    )

    def fake_run_stock_gold(symbol, storage=None):
        captured.append((symbol, storage))

        return {
            "symbol": symbol,
            "success": True,
            "skipped": True,
            "reason": "Test",
        }

    monkeypatch.setattr(
        coordinator,
        "run_stock_gold",
        fake_run_stock_gold,
    )

    results = coordinator.run_twelvedata_gold(
        storage=storage,
    )

    assert len(results) == 2
    assert captured == [
        ("NVDA", storage),
        ("AAPL", storage),
    ]
