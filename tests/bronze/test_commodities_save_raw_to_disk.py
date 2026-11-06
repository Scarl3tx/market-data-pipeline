import polars as pl
import pytest
from datetime import datetime, timezone

import src.alphavantage_trackers.pipelines.av_commodities.bronze.extract_alphavantage_commodities_data as commodities_bronze


@pytest.fixture
def commodities_setup(tmp_path, monkeypatch):
    """Use a temporary directory and fixed UTC timestamp for testing."""

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(
                2026, 11, 4, 12, 0, 0,
                tzinfo=timezone.utc
            )

    monkeypatch.setattr(
        commodities_bronze,
        "COMMODITIES_PATH",
        tmp_path
    )

    monkeypatch.setattr(
        commodities_bronze,
        "datetime",
        FixedDateTime
    )

    return tmp_path


def test_save_raw_to_disk_success(commodities_setup):
    data = {
        "nominal": "XAUUSD",
        "data": [
            {"date": "2026-11-03", "value": "3900.50"},
            {"date": "2026-11-04", "value": "3910.25"}
        ]
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is True

    file_path = (
        commodities_setup
        / "XAU"
        / "20261104_12_XAU_raw.pq"
    )

    assert file_path.exists()

    df = pl.read_parquet(file_path)

    assert df.height == 2
    assert df.columns == ["nominal", "data"]

    assert df["nominal"].to_list() == [
        "XAUUSD",
        "XAUUSD"
    ]

    assert df["data"].struct.field("date").to_list() == [
        "2026-11-03",
        "2026-11-04"
    ]

    assert df["data"].struct.field("price").to_list() == [
        "3900.50",
        "3910.25"
    ]


def test_save_raw_to_disk_price_field(commodities_setup):
    """Verify that observations using 'price' are saved correctly."""

    data = {
        "nominal": "XAUUSD",
        "data": [
            {"date": "2026-11-03", "price": "3900.50"}
        ]
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is True

    file_path = (
        commodities_setup
        / "XAU"
        / "20261104_12_XAU_raw.pq"
    )

    df = pl.read_parquet(file_path)

    assert df.height == 1
    assert df["data"].struct.field("price").to_list() == ["3900.50"]


def test_save_raw_to_disk_skips_malformed_observations(commodities_setup):
    """Verify malformed observations are skipped while valid ones are saved."""

    data = {
        "nominal": "XAUUSD",
        "data": [
            {"date": "2026-11-03", "value": "3900.50"},
            {"date": "2026-11-04"},
            {"value": "3910.25"},
            "invalid observation",
            {"date": "2026-11-05", "value": "3920.75"}
        ]
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is True

    file_path = (
        commodities_setup
        / "XAU"
        / "20261104_12_XAU_raw.pq"
    )

    df = pl.read_parquet(file_path)

    assert df.height == 2

    assert df["data"].struct.field("date").to_list() == [
        "2026-11-03",
        "2026-11-05"
    ]


def test_save_raw_to_disk_empty_payload(commodities_setup):
    """An empty payload should not produce a file."""

    result = commodities_bronze.save_raw_to_disk("XAU", {})

    assert result is False
    assert not (commodities_setup / "XAU").exists()


def test_save_raw_to_disk_missing_keys(commodities_setup):
    """Payloads missing required keys should fail."""

    data = {
        "nominal": "XAUUSD"
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is False


def test_save_raw_to_disk_empty_observations(commodities_setup):
    """An empty observations list should fail."""

    data = {
        "nominal": "XAUUSD",
        "data": []
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is False


def test_save_raw_to_disk_no_valid_records(commodities_setup):
    """Observations without valid dates or prices should not be saved."""

    data = {
        "nominal": "XAUUSD",
        "data": [
            {"date": "2026-11-03"},
            {"value": "3900.50"},
            {"date": None, "value": "3910.25"}
        ]
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is False


def test_save_raw_to_disk_existing_file(commodities_setup):
    """An existing hourly file should not be overwritten."""

    asset_dir = commodities_setup / "XAU"
    asset_dir.mkdir(parents=True)

    file_path = asset_dir / "20261104_12_XAU_raw.pq"

    original_data = pl.DataFrame({
        "existing": ["original"]
    })

    original_data.write_parquet(file_path)

    data = {
        "nominal": "XAUUSD",
        "data": [
            {"date": "2026-11-03", "value": "3900.50"}
        ]
    }

    result = commodities_bronze.save_raw_to_disk("XAU", data)

    assert result is True

    df = pl.read_parquet(file_path)

    assert df.columns == ["existing"]
    assert df["existing"].to_list() == ["original"]