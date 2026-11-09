
from datetime import datetime, timedelta

import polars as pl
from polars.testing import assert_frame_equal

from market_data.maintenance.twelvedata import (
    compact_twelvedata_bronze,
)
from market_data.pipelines.twelvedata.silver.storage import (
    load_stock_bronze_data,
)
from market_data.pipelines.twelvedata.silver.transform import (
    transform_stock_silver,
)


def _make_stock_data(start, end, price_adjustment=0.0):
    rows = []

    for index in range(start, end):
        trading_date = datetime(2020, 1, 1) + timedelta(days=index)
        close = 100.0 + index * 0.5 + price_adjustment

        rows.append({
            "asset_name": "NVDA",
            "date": trading_date,
            "open": close - 1.0,
            "high": close + 2.0,
            "low": close - 2.0,
            "close": close,
            "volume": 10000 + index,
        })

    return pl.DataFrame(rows).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


def test_compacted_bronze_produces_identical_silver(tmp_path):
    bronze_path = tmp_path / "bronze"
    symbol_path = bronze_path / "stocks" / "NVDA"
    symbol_path.mkdir(parents=True)

    first_file = symbol_path / (
        "NVDA_20200101_20200519_raw.pq"
    )

    second_file = symbol_path / (
        "NVDA_20200430_20200827_raw.pq"
    )

    # The second file overlaps the first file by 20 dates.
    # Its values differ, allowing us to verify keep-last behaviour.
    first_data = _make_stock_data(0, 140)
    second_data = _make_stock_data(120, 240, price_adjustment=10.0)

    first_data.write_parquet(first_file)
    second_data.write_parquet(second_file)

    # Establish expected Silver output before compaction.
    bronze_before = load_stock_bronze_data(
        bronze_path=bronze_path,
        symbol="NVDA",
    )

    expected_silver = transform_stock_silver(bronze_before)

    assert bronze_before.height == 240
    assert expected_silver.height == 240

    # Compact Bronze and remove original files.
    result = compact_twelvedata_bronze(
        bronze_path=bronze_path,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "success"

    nvda_result = result["results"]["NVDA"]

    assert nvda_result["files_found"] == 2
    assert nvda_result["files_removed"] == 2
    assert nvda_result["rows_after"] == 240

    remaining_files = list(
        symbol_path.glob("NVDA_*_raw.pq")
    )

    assert len(remaining_files) == 1

    # Load the consolidated Bronze file through the real loader.
    bronze_after = load_stock_bronze_data(
        bronze_path=bronze_path,
        symbol="NVDA",
    )

    actual_silver = transform_stock_silver(bronze_after)

    # Verify the entire Silver dataset is unchanged.
    assert_frame_equal(
        actual_silver,
        expected_silver,
        check_exact=True,
    )
