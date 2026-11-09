
from datetime import date

import polars as pl

from market_data.maintenance.twelvedata import (
    compact_twelvedata_bronze,
)


def _write_batch(path, dates):
    path.parent.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "date": dates,
            "open": [100.0] * len(dates),
            "high": [110.0] * len(dates),
            "low": [90.0] * len(dates),
            "close": [105.0] * len(dates),
            "volume": [1000] * len(dates),
        }
    ).write_parquet(path)


def test_dry_run_discovers_symbol_files(tmp_path):
    symbol_path = tmp_path / "stocks" / "NVDA"

    _write_batch(
        symbol_path / "NVDA_20200101_20200102_raw.pq",
        [date(2020, 1, 1), date(2020, 1, 2)],
    )

    _write_batch(
        symbol_path / "NVDA_20200103_20200104_raw.pq",
        [date(2020, 1, 3), date(2020, 1, 4)],
    )

    result = compact_twelvedata_bronze(
        bronze_path=tmp_path,
        dry_run=True,
    )

    nvda = result["results"]["NVDA"]

    assert result["status"] == "dry_run"
    assert nvda["files_found"] == 2
    assert nvda["status"] == "dry_run"
    assert nvda["output_path"].name == (
        "NVDA_20200101_20200104_raw.pq"
    )
    assert len(list(symbol_path.glob("*_raw.pq"))) == 2


def test_compaction_merges_and_removes_source_files(tmp_path):
    symbol_path = tmp_path / "stocks" / "NVDA"

    _write_batch(
        symbol_path / "NVDA_20200101_20200102_raw.pq",
        [date(2020, 1, 1), date(2020, 1, 2)],
    )

    _write_batch(
        symbol_path / "NVDA_20200102_20200103_raw.pq",
        [date(2020, 1, 2), date(2020, 1, 3)],
    )

    result = compact_twelvedata_bronze(
        bronze_path=tmp_path,
        dry_run=False,
        delete_sources=True,
    )

    nvda = result["results"]["NVDA"]

    assert result["status"] == "success"
    assert nvda["status"] == "success"
    assert nvda["rows_before"] == 4
    assert nvda["rows_after"] == 3
    assert nvda["files_removed"] == 2

    output = symbol_path / "NVDA_20200101_20200103_raw.pq"

    assert output.exists()

    saved = pl.read_parquet(output)

    assert saved.height == 3
    assert saved["date"].is_sorted()


def test_symbol_with_one_file_is_skipped(tmp_path):
    symbol_path = tmp_path / "stocks" / "NVDA"

    _write_batch(
        symbol_path / "NVDA_20200101_20200102_raw.pq",
        [date(2020, 1, 1), date(2020, 1, 2)],
    )

    result = compact_twelvedata_bronze(
        bronze_path=tmp_path,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "skipped"
    assert result["results"]["NVDA"]["status"] == "skipped"
    assert len(list(symbol_path.glob("*_raw.pq"))) == 1



def test_failed_symbol_does_not_stop_other_symbols(tmp_path):
    nvda_path = tmp_path / "stocks" / "NVDA"
    nvda_path.mkdir(parents=True, exist_ok=True)

    nvda_file_1 = nvda_path / "NVDA_20200101_20200102_raw.pq"
    nvda_file_2 = nvda_path / "NVDA_20200103_20200104_raw.pq"

    nvda_file_1.write_text("invalid parquet")
    nvda_file_2.write_text("invalid parquet")

    aapl_path = tmp_path / "stocks" / "AAPL"

    _write_batch(
        aapl_path / "AAPL_20200101_20200102_raw.pq",
        [date(2020, 1, 1)],
    )

    _write_batch(
        aapl_path / "AAPL_20200103_20200104_raw.pq",
        [date(2020, 1, 2)],
    )

    result = compact_twelvedata_bronze(
        bronze_path=tmp_path,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "partial"

    # NVDA fails, but its original files remain untouched.
    assert result["results"]["NVDA"]["status"] == "failed"
    assert nvda_file_1.exists()
    assert nvda_file_2.exists()

    # AAPL compaction succeeds independently.
    assert result["results"]["AAPL"]["status"] == "success"
    assert result["results"]["AAPL"]["files_removed"] == 1

    aapl_output = (
        aapl_path / "AAPL_20200101_20200102_raw.pq"
    )

    assert aapl_output.exists()

    saved = pl.read_parquet(aapl_output)

    assert saved.height == 2
    assert saved["date"].is_sorted()
