
import polars as pl
import pytest

import src.alphavantage_trackers.pipelines.av_commodities.silver.silver_transform_alphavantage_commodities as commodities_silver


@pytest.fixture
def raw_commodities_df():
    return pl.DataFrame({
        "nominal": ["USD"],
        "data": [[
            {"date": "2026-11-03", "price": "45.50"},
            {"date": "2026-11-04", "price": "46.20"}
        ]]
    })


@pytest.fixture
def commodity_schema():
    return {
        "asset_name": "str",
        "date": "datetime64[ns]",
        "close": "float64"
    }


@pytest.fixture
def valid_commodity_df():
    return pl.DataFrame({
        "asset_name": ["XAU", "XAU"],
        "date": [
            "2026-11-03",
            "2026-11-04"
        ],
        "close": [45.50, 46.20]
    })


# -------------------------------------------------------------------
# Raw Parquet reader
# -------------------------------------------------------------------

def test_pq_to_df_reads_latest_file(monkeypatch, tmp_path):
    older = tmp_path / "20261103_12_XAU_raw.pq"
    newer = tmp_path / "20261104_12_XAU_raw.pq"

    older.touch()
    newer.touch()

    monkeypatch.setattr(
        commodities_silver,
        "RAW_PATH",
        tmp_path
    )

    expected_df = pl.DataFrame({"date": ["2026-11-04"]})

    monkeypatch.setattr(
        pl,
        "read_parquet",
        lambda path: expected_df
    )

    result = commodities_silver.pq_to_df("XAU")

    assert result.equals(expected_df)


def test_pq_to_df_returns_empty_when_no_files_exist(
    monkeypatch,
    tmp_path
):
    monkeypatch.setattr(
        commodities_silver,
        "RAW_PATH",
        tmp_path
    )

    result = commodities_silver.pq_to_df("XAU")

    assert result.is_empty()


# -------------------------------------------------------------------
# Flattening
# -------------------------------------------------------------------

def test_flatten_commodity_gold_success(raw_commodities_df):
    result = commodities_silver.flatten_alphavantage_ohlcv(
        "XAU",
        raw_commodities_df,
        {"schema_type": "commodity_gold"}
    )

    assert result.height == 2
    assert result["asset_name"].to_list() == ["XAU", "XAU"]
    assert result["close"].to_list() == [45.50, 46.20]
    assert "price" not in result.columns
    assert "date" in result.columns


def test_flatten_returns_empty_for_empty_dataframe():
    result = commodities_silver.flatten_alphavantage_ohlcv(
        "XAU",
        pl.DataFrame(),
        {"schema_type": "commodity_gold"}
    )

    assert result.is_empty()


def test_flatten_returns_empty_for_unsupported_schema(
    raw_commodities_df
):
    result = commodities_silver.flatten_alphavantage_ohlcv(
        "XAU",
        raw_commodities_df,
        {"schema_type": "unsupported"}
    )

    assert result.is_empty()


def test_flatten_returns_empty_for_no_observations():
    df = pl.DataFrame({
        "data": [None]
    })

    result = commodities_silver.flatten_alphavantage_ohlcv(
        "XAU",
        df,
        {"schema_type": "commodity_gold"}
    )

    assert result.is_empty()


# -------------------------------------------------------------------
# Schema validation
# -------------------------------------------------------------------

def test_df_column_check_success(
    valid_commodity_df,
    commodity_schema
):
    result = commodities_silver.df_column_check(
        valid_commodity_df,
        commodity_schema
    )

    assert not result.is_empty()
    assert result.columns == list(commodity_schema.keys())
    assert result.schema["asset_name"] == pl.String
    assert result.schema["close"] == pl.Float64


def test_df_column_check_rejects_missing_columns(
    valid_commodity_df,
    commodity_schema
):
    invalid_schema = {
        **commodity_schema,
        "volume": "int64"
    }

    result = commodities_silver.df_column_check(
        valid_commodity_df,
        invalid_schema
    )

    assert result.is_empty()


def test_df_column_check_rejects_invalid_numeric_values(
    commodity_schema
):
    df = pl.DataFrame({
        "asset_name": ["XAU"],
        "date": ["2026-11-03"],
        "close": ["invalid"]
    })

    result = commodities_silver.df_column_check(
        df,
        commodity_schema
    )

    assert result.is_empty()


def test_df_column_check_converts_nan_to_null(
    commodity_schema
):
    df = pl.DataFrame({
        "asset_name": ["XAU"],
        "date": ["2026-11-03"],
        "close": [float("nan")]
    })

    result = commodities_silver.df_column_check(
        df,
        commodity_schema
    )

    assert not result.is_empty()
    assert result["close"].null_count() == 1
    assert result["close"][0] is None


def test_df_column_check_returns_empty_for_empty_input(
    commodity_schema
):
    result = commodities_silver.df_column_check(
        pl.DataFrame(),
        commodity_schema
    )

    assert result.is_empty()


# -------------------------------------------------------------------
# Time attributes
# -------------------------------------------------------------------

def test_derive_time_attributes(valid_commodity_df):
    result = commodities_silver.derive_time_attributes(
        valid_commodity_df
    )

    assert result["year"].to_list() == [2026, 2026]
    assert result["month"].to_list() == [11, 11]
    assert result["day"].to_list() == [3, 4]
    assert result["day_of_week"].to_list() == [1, 2]


# -------------------------------------------------------------------
# Integrity check
# -------------------------------------------------------------------

def test_df_integrity_check_skips_when_ohlc_absent(
    valid_commodity_df
):
    result = commodities_silver.df_integrity_check(
        valid_commodity_df
    )

    assert result.equals(valid_commodity_df)


def test_df_integrity_check_returns_empty_input_unchanged():
    df = pl.DataFrame()

    result = commodities_silver.df_integrity_check(df)

    assert result.equals(df)


# -------------------------------------------------------------------
# Silver writer
# -------------------------------------------------------------------

def test_save_silver_to_pq_success(monkeypatch, tmp_path):
    monkeypatch.setattr(
        commodities_silver,
        "COMMODITIES_SILVER_PATH",
        tmp_path
    )

    df = pl.DataFrame({
        "asset_name": ["XAU"],
        "close": [45.50]
    })

    result = commodities_silver.save_silver_to_pq(
        "XAU",
        df
    )

    output_path = tmp_path / "XAU" / "XAU_silver.pq"

    assert result is None
    assert output_path.exists()

    saved_df = pl.read_parquet(output_path)

    assert saved_df.equals(df)


def test_save_silver_to_pq_rejects_empty_dataframe():
    with pytest.raises(ValueError):
        commodities_silver.save_silver_to_pq(
            "XAU",
            pl.DataFrame()
        )

@pytest.fixture
def silver_output_schema():
    return {
        "asset_name": "str",
        "date": "datetime64[ns]",
        "year": "int64",
        "month": "int64",
        "day": "int64",
        "day_of_week": "int64",
        "close": "float64"
    }


# -------------------------------------------------------------------
# Full Commodities Silver pipeline tests
# -------------------------------------------------------------------

@pytest.fixture
def commodity_configs():
    return {
        "XAU": {
            "function": "GOLD_SILVER_SPOT",
            "schema_type": "commodity_gold"
        }
    }


@pytest.fixture
def commodity_asset_schemas(commodity_schema):
    return {
        "commodity_gold": commodity_schema
    }


def test_run_silver_pipeline_success(
    monkeypatch,
    raw_commodities_df,
    commodity_configs,
    commodity_asset_schemas,
    silver_output_schema
):
    saved_data = {}

    monkeypatch.setattr(
        commodities_silver,
        "pq_to_df",
        lambda asset_name: raw_commodities_df
    )

    def mock_save(asset_name, df):
        saved_data[asset_name] = df

    monkeypatch.setattr(
        commodities_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = commodities_silver.run_alphavantage_commodities_silver(
        configs=commodity_configs,
        asset_schemas=commodity_asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is True
    assert "XAU" in saved_data

    saved_df = saved_data["XAU"]

    assert saved_df.height == 2
    assert saved_df.columns == list(silver_output_schema.keys())
    assert saved_df["asset_name"].to_list() == ["XAU", "XAU"]
    assert saved_df["close"].to_list() == [45.50, 46.20]


def test_run_silver_pipeline_no_raw_data(
    monkeypatch,
    commodity_configs,
    commodity_asset_schemas,
    silver_output_schema
):
    save_called = False

    monkeypatch.setattr(
        commodities_silver,
        "pq_to_df",
        lambda asset_name: pl.DataFrame()
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True

    monkeypatch.setattr(
        commodities_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = commodities_silver.run_alphavantage_commodities_silver(
        configs=commodity_configs,
        asset_schemas=commodity_asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_invalid_source_schema(
    monkeypatch,
    raw_commodities_df,
    commodity_configs,
    silver_output_schema
):
    save_called = False

    monkeypatch.setattr(
        commodities_silver,
        "pq_to_df",
        lambda asset_name: raw_commodities_df
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True

    monkeypatch.setattr(
        commodities_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = commodities_silver.run_alphavantage_commodities_silver(
        configs=commodity_configs,
        asset_schemas={},
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_invalid_output_schema(
    monkeypatch,
    raw_commodities_df,
    commodity_configs,
    commodity_asset_schemas
):
    save_called = False

    invalid_output_schema = {
        "asset_name": "str",
        "date": "datetime64[ns]",
        "year": "int64",
        "month": "int64",
        "day": "int64",
        "day_of_week": "int64",
        "close": "str",
        "missing_column": "float64"
    }

    monkeypatch.setattr(
        commodities_silver,
        "pq_to_df",
        lambda asset_name: raw_commodities_df
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True

    monkeypatch.setattr(
        commodities_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = commodities_silver.run_alphavantage_commodities_silver(
        configs=commodity_configs,
        asset_schemas=commodity_asset_schemas,
        silver_output_schema=invalid_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_continues_after_asset_failure(
    monkeypatch,
    raw_commodities_df,
    commodity_asset_schemas,
    silver_output_schema
):
    configs = {
        "XAU": {
            "function": "GOLD_SILVER_SPOT",
            "schema_type": "commodity_gold"
        },
        "XAG": {
            "function": "GOLD_SILVER_SPOT",
            "schema_type": "commodity_gold"
        }
    }

    saved_assets = []

    def mock_pq_to_df(asset_name):
        if asset_name == "XAU":
            return pl.DataFrame()

        return raw_commodities_df

    def mock_save(asset_name, df):
        saved_assets.append(asset_name)

    monkeypatch.setattr(
        commodities_silver,
        "pq_to_df",
        mock_pq_to_df
    )

    monkeypatch.setattr(
        commodities_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = commodities_silver.run_alphavantage_commodities_silver(
        configs=configs,
        asset_schemas=commodity_asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert "XAG" in saved_assets
    assert "XAU" not in saved_assets

