
import polars as pl
import pytest

import src.alphavantage_trackers.pipelines.av_stocks.silver.silver_transform_alphavantage_stocks as stock_silver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_stock_df():
    """Mimic the nested structure written by the Stock Bronze pipeline."""

    timeseries_schema = pl.Struct([
        pl.Field("1. open", pl.String),
        pl.Field("2. high", pl.String),
        pl.Field("3. low", pl.String),
        pl.Field("4. close", pl.String),
        pl.Field("5. volume", pl.String),
    ])

    return pl.DataFrame(
        {
            "Meta Data": [
                "2026-11-03",
                "2026-11-04",
            ],
            "Time Series (Daily)": [
                {
                    "1. open": "100.0",
                    "2. high": "105.0",
                    "3. low": "99.0",
                    "4. close": "103.0",
                    "5. volume": "1000000",
                },
                {
                    "1. open": "103.0",
                    "2. high": "108.0",
                    "3. low": "102.0",
                    "4. close": "107.0",
                    "5. volume": "1200000",
                },
            ],
        },
        schema={
            "Meta Data": pl.String,
            "Time Series (Daily)": timeseries_schema,
        },
    )


@pytest.fixture
def stock_schema():
    return {
        "asset_name": "str",
        "date": "datetime",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int",
    }


@pytest.fixture
def valid_ohlcv_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            "2026-11-03",
            "2026-11-04",
        ],
        "open": [100.0, 103.0],
        "high": [105.0, 108.0],
        "low": [99.0, 102.0],
        "close": [103.0, 107.0],
        "volume": [1000000, 1200000],
    }).with_columns(
        pl.col("date").str.to_datetime()
    )


# ---------------------------------------------------------------------------
# 1. Bronze Parquet reader
# ---------------------------------------------------------------------------

def test_pq_to_df_reads_latest_file(tmp_path, monkeypatch):
    monkeypatch.setattr(stock_silver, "RAW_PATH", tmp_path)

    older_dir = tmp_path / "NVDA"
    older_dir.mkdir()

    older_file = older_dir / "20261103_12_NVDA_raw.pq"
    newer_file = older_dir / "20261104_12_NVDA_raw.pq"

    pl.DataFrame({"value": [1]}).write_parquet(older_file)
    pl.DataFrame({"value": [2]}).write_parquet(newer_file)

    result = stock_silver.pq_to_df("NVDA")

    assert result.height == 1
    assert result["value"].to_list() == [2]


def test_pq_to_df_returns_empty_when_no_files_exist(tmp_path, monkeypatch):
    monkeypatch.setattr(stock_silver, "RAW_PATH", tmp_path)

    result = stock_silver.pq_to_df("NVDA")

    assert result.is_empty()


# ---------------------------------------------------------------------------
# 2. Alpha Vantage flattening
# ---------------------------------------------------------------------------

def test_flatten_equity_daily_success(raw_stock_df):
    result = stock_silver.flatten_alphavantage_ohlcv(
        "NVDA",
        raw_stock_df,
        {"schema_type": "equity_daily"},
    )

    assert result.height == 2

    assert result.columns == [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "asset_name",
    ]

    assert result["asset_name"].to_list() == ["NVDA", "NVDA"]

    assert result["date"].to_list() == [
        # Polars returns Python datetime values here.
        result["date"][0],
        result["date"][1],
    ]

    assert result["open"].to_list() == [100.0, 103.0]
    assert result["high"].to_list() == [105.0, 108.0]
    assert result["low"].to_list() == [99.0, 102.0]
    assert result["close"].to_list() == [103.0, 107.0]
    assert result["volume"].to_list() == [1000000.0, 1200000.0]


def test_flatten_returns_empty_for_empty_dataframe():
    result = stock_silver.flatten_alphavantage_ohlcv(
        "NVDA",
        pl.DataFrame(),
        {"schema_type": "equity_daily"},
    )

    assert result.is_empty()


def test_flatten_returns_empty_for_missing_equity_columns():
    raw_df = pl.DataFrame({
        "date": ["2026-11-04"],
        "close": [100.0],
    })

    result = stock_silver.flatten_alphavantage_ohlcv(
        "NVDA",
        raw_df,
        {"schema_type": "equity_daily"},
    )

    assert result.is_empty()


def test_flatten_returns_empty_for_unsupported_schema(raw_stock_df):
    result = stock_silver.flatten_alphavantage_ohlcv(
        "NVDA",
        raw_stock_df,
        {"schema_type": "unsupported"},
    )

    assert result.is_empty()


# ---------------------------------------------------------------------------
# 3. Schema validation
# ---------------------------------------------------------------------------

def test_df_column_check_success(valid_ohlcv_df, stock_schema):
    result = stock_silver.df_column_check(
        valid_ohlcv_df,
        stock_schema,
    )

    assert result.height == 2
    assert result.columns == list(stock_schema.keys())

    assert result.schema["asset_name"] == pl.String
    assert result.schema["date"] == pl.Datetime("us")
    assert result.schema["open"] == pl.Float64
    assert result.schema["volume"] == pl.Int64


def test_df_column_check_rejects_missing_columns(valid_ohlcv_df):
    schema = {
        "asset_name": "str",
        "date": "datetime",
        "open": "float",
        "missing_column": "float",
    }

    result = stock_silver.df_column_check(
        valid_ohlcv_df,
        schema,
    )

    assert result.is_empty()


def test_df_column_check_rejects_invalid_numeric_values():
    df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": ["2026-11-04"],
        "open": ["not-a-number"],
    })

    schema = {
        "asset_name": "str",
        "date": "datetime",
        "open": "float",
    }

    result = stock_silver.df_column_check(df, schema)

    assert result.is_empty()


def test_df_column_check_converts_nan_to_null():
    df = pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            "2026-11-03",
            "2026-11-04",
        ],
        "close": [100.0, float("nan")],
    })

    schema = {
        "asset_name": "str",
        "date": "datetime",
        "close": "float",
    }

    result = stock_silver.df_column_check(df, schema)

    assert result.height == 2
    assert result["close"].to_list() == [100.0, None]


def test_df_column_check_returns_empty_for_empty_input():
    result = stock_silver.df_column_check(
        pl.DataFrame(),
        {"asset_name": "str"},
    )

    assert result.is_empty()


# ---------------------------------------------------------------------------
# 4. Time attributes
# ---------------------------------------------------------------------------

def test_derive_time_attributes(valid_ohlcv_df):
    result = stock_silver.derive_time_attributes(valid_ohlcv_df)

    assert result["year"].to_list() == [2026, 2026]
    assert result["month"].to_list() == [11, 11]
    assert result["day"].to_list() == [3, 4]

    # Tuesday = 1, Wednesday = 2.
    assert result["day_of_week"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# 5. OHLC integrity
# ---------------------------------------------------------------------------

def test_df_integrity_check_removes_invalid_rows():
    df = pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            "2026-11-03",
            "2026-11-04",
        ],
        "open": [100.0, 100.0],
        "high": [105.0, 95.0],
        "low": [99.0, 90.0],
        "close": [103.0, 94.0],
    })

    result = stock_silver.df_integrity_check(df)

    assert result.height == 1
    assert result["date"].to_list() == ["2026-11-03"]


def test_df_integrity_check_returns_empty_input_unchanged():
    df = pl.DataFrame()

    result = stock_silver.df_integrity_check(df)

    assert result.equals(df)


# ---------------------------------------------------------------------------
# 6. Silver Parquet writer
# ---------------------------------------------------------------------------

def test_save_silver_to_pq_success(tmp_path, monkeypatch, valid_ohlcv_df):
    monkeypatch.setattr(
        stock_silver,
        "STOCKS_SILVER_PATH",
        tmp_path,
    )

    stock_silver.save_silver_to_pq("NVDA", valid_ohlcv_df)

    file_path = tmp_path / "NVDA" / "NVDA_silver.pq"

    assert file_path.exists()

    saved_df = pl.read_parquet(file_path)

    assert saved_df.equals(valid_ohlcv_df)


def test_save_silver_to_pq_rejects_empty_dataframe(tmp_path, monkeypatch):
    monkeypatch.setattr(
        stock_silver,
        "STOCKS_SILVER_PATH",
        tmp_path,
    )

    with pytest.raises(ValueError):
        stock_silver.save_silver_to_pq(
            "NVDA",
            pl.DataFrame(),
        )

    assert not (tmp_path / "NVDA").exists()

# -------------------------------------------------------------------
# Full Silver pipeline tests
# -------------------------------------------------------------------

@pytest.fixture
def silver_output_schema():
    return {
        "asset_name": "str",
        "date": "datetime64[ns]",
        "year": "int64",
        "month": "int64",
        "day": "int64",
        "day_of_week": "int64",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64"
    }


@pytest.fixture
def asset_configs():
    return {
        "NVDA": {
            "function": "TIME_SERIES_DAILY",
            "symbol_param": "symbol",
            "schema_type": "equity_daily"
        }
    }


@pytest.fixture
def asset_schemas(stock_schema):
    return {
        "equity_daily": stock_schema
    }


def test_run_silver_pipeline_success(
    monkeypatch,
    raw_stock_df,
    asset_configs,
    asset_schemas,
    silver_output_schema
):
    saved_data = {}

    monkeypatch.setattr(
        stock_silver,
        "pq_to_df",
        lambda asset_name: raw_stock_df
    )

    def mock_save(asset_name, df):
        saved_data[asset_name] = df
        return True

    monkeypatch.setattr(
        stock_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = stock_silver.run_alphavantage_stocks_silver(
        configs=asset_configs,
        asset_schemas=asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is True
    assert "NVDA" in saved_data

    saved_df = saved_data["NVDA"]

    assert saved_df.height == 2
    assert saved_df.columns == list(silver_output_schema.keys())


def test_run_silver_pipeline_no_raw_data(
    monkeypatch,
    asset_configs,
    asset_schemas,
    silver_output_schema
):
    save_called = False

    monkeypatch.setattr(
        stock_silver,
        "pq_to_df",
        lambda asset_name: pl.DataFrame()
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True
        return True

    monkeypatch.setattr(
        stock_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = stock_silver.run_alphavantage_stocks_silver(
        configs=asset_configs,
        asset_schemas=asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_invalid_source_schema(
    monkeypatch,
    raw_stock_df,
    asset_configs,
    silver_output_schema
):
    save_called = False

    monkeypatch.setattr(
        stock_silver,
        "pq_to_df",
        lambda asset_name: raw_stock_df
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True
        return True

    monkeypatch.setattr(
        stock_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = stock_silver.run_alphavantage_stocks_silver(
        configs=asset_configs,
        asset_schemas={},
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_invalid_ohlcv(
    monkeypatch,
    asset_configs,
    asset_schemas,
    silver_output_schema
):
    # High is lower than open, making the row invalid.
    invalid_df = pl.DataFrame({
        "asset_name": ["NVDA"],
        "date": ["2026-11-04"],
        "open": [150.0],
        "high": [140.0],
        "low": [130.0],
        "close": [135.0],
        "volume": [1000]
    })

    save_called = False

    monkeypatch.setattr(
        stock_silver,
        "pq_to_df",
        lambda asset_name: invalid_df
    )

    def mock_save(asset_name, df):
        nonlocal save_called
        save_called = True
        return True

    monkeypatch.setattr(
        stock_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = stock_silver.run_alphavantage_stocks_silver(
        configs=asset_configs,
        asset_schemas=asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert save_called is False


def test_run_silver_pipeline_continues_after_asset_failure(
    monkeypatch,
    raw_stock_df,
    silver_output_schema
):
    configs = {
        "NVDA": {
            "function": "TIME_SERIES_DAILY",
            "symbol_param": "symbol",
            "schema_type": "equity_daily"
        },
        "AAPL": {
            "function": "TIME_SERIES_DAILY",
            "symbol_param": "symbol",
            "schema_type": "equity_daily"
        }
    }

    asset_schemas = {
        "equity_daily": {
            "asset_name": "str",
            "date": "datetime64[ns]",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "int64"
        }
    }

    saved_assets = []

    def mock_pq_to_df(asset_name):
        if asset_name == "NVDA":
            return pl.DataFrame()

        return raw_stock_df

    def mock_save(asset_name, df):
        saved_assets.append(asset_name)
        return True

    monkeypatch.setattr(
        stock_silver,
        "pq_to_df",
        mock_pq_to_df
    )

    monkeypatch.setattr(
        stock_silver,
        "save_silver_to_pq",
        mock_save
    )

    result = stock_silver.run_alphavantage_stocks_silver(
        configs=configs,
        asset_schemas=asset_schemas,
        silver_output_schema=silver_output_schema
    )

    assert result is False
    assert "AAPL" in saved_assets
    assert "NVDA" not in saved_assets



