
from datetime import datetime

import polars as pl
import pytest

from market_data.pipelines.twelvedata.gold.quarantine import (
    save_rejected_stock_data,
)


def make_rejected_df(
    dates=None,
    errors=None,
):
    if dates is None:
        dates = [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ]

    if errors is None:
        errors = [
            "invalid_open",
            "invalid_volume",
        ]

    return pl.DataFrame({
        "asset_name": ["NVDA"] * len(dates),
        "date": dates,
        "open": [-1.0] * len(dates),
        "high": [105.0] * len(dates),
        "low": [98.0] * len(dates),
        "close": [103.0] * len(dates),
        "volume": [-100] * len(dates),
        "validation_errors": errors,
    })


def test_save_rejected_records_creates_file(tmp_path):
    df = make_rejected_df()

    saved = save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=df,
        quarantine_path=tmp_path,
    )

    quarantine_file = tmp_path / "NVDA_rejected.pq"

    assert saved == 2
    assert quarantine_file.exists()

    stored = pl.read_parquet(quarantine_file)

    assert stored.height == 2
    assert "validation_errors" in stored.columns


def test_save_rejected_records_appends_new_dates(tmp_path):
    first_batch = make_rejected_df(
        dates=[datetime(2026, 11, 5)],
        errors=["invalid_open"],
    )

    second_batch = make_rejected_df(
        dates=[datetime(2026, 11, 6)],
        errors=["invalid_volume"],
    )

    save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=first_batch,
        quarantine_path=tmp_path,
    )

    saved = save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=second_batch,
        quarantine_path=tmp_path,
    )

    stored = pl.read_parquet(
        tmp_path / "NVDA_rejected.pq"
    )

    assert saved == 1
    assert stored.height == 2


def test_save_rejected_records_deduplicates_existing_date(tmp_path):
    first_batch = make_rejected_df(
        dates=[datetime(2026, 11, 5)],
        errors=["invalid_open"],
    )

    repeated_batch = make_rejected_df(
        dates=[datetime(2026, 11, 5)],
        errors=["invalid_close"],
    )

    save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=first_batch,
        quarantine_path=tmp_path,
    )

    saved = save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=repeated_batch,
        quarantine_path=tmp_path,
    )

    stored = pl.read_parquet(
        tmp_path / "NVDA_rejected.pq"
    )

    assert saved == 0
    assert stored.height == 1
    assert stored.item(0, "validation_errors") == "invalid_close"


def test_save_rejected_records_returns_zero_for_empty_dataframe(
    tmp_path,
):
    df = pl.DataFrame(
        schema={
            "asset_name": pl.String,
            "date": pl.Datetime,
            "validation_errors": pl.String,
        }
    )

    saved = save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=df,
        quarantine_path=tmp_path,
    )

    assert saved == 0
    assert not (tmp_path / "NVDA_rejected.pq").exists()


def test_save_rejected_records_rejects_missing_columns(tmp_path):
    df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": [datetime(2026, 11, 5)],
    })

    with pytest.raises(
        ValueError,
        match="Missing quarantine columns",
    ):
        save_rejected_stock_data(
            symbol="NVDA",
            rejected_df=df,
            quarantine_path=tmp_path,
        )



def test_quarantine_files_are_separate_by_symbol(tmp_path):
    nvda_df = make_rejected_df(
        dates=[datetime(2026, 11, 5)],
        errors=["invalid_open"],
    )

    save_rejected_stock_data(
        symbol="NVDA",
        rejected_df=nvda_df,
        quarantine_path=tmp_path,
    )

    aapl_df = nvda_df.with_columns(
        pl.lit("AAPL").alias("asset_name")
    )

    save_rejected_stock_data(
        symbol="AAPL",
        rejected_df=aapl_df,
        quarantine_path=tmp_path,
    )

    assert (tmp_path / "NVDA_rejected.pq").exists()
    assert (tmp_path / "AAPL_rejected.pq").exists()

    nvda_stored = pl.read_parquet(
        tmp_path / "NVDA_rejected.pq"
    )
    aapl_stored = pl.read_parquet(
        tmp_path / "AAPL_rejected.pq"
    )

    assert nvda_stored["asset_name"].unique().to_list() == ["NVDA"]
    assert aapl_stored["asset_name"].unique().to_list() == ["AAPL"]
