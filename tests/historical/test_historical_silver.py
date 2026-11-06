import polars as pl

from src.historical.transformation import stocks

def create_test_dataframe(dates: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        "asset_name": ["NVDA"] * len(dates),
        "date": pl.Series(dates).str.to_datetime(time_unit="ns"),
        "open": [100.0] * len(dates),
        "high": [110.0] * len(dates),
        "low": [90.0] * len(dates),
        "close": [105.0] * len(dates),
        "volume": [1000] * len(dates),
    })


def test_multiple_batches_are_combined_and_sorted(tmp_path, monkeypatch):
    bronze_path = tmp_path / "historical" / "stocks"
    silver_path = tmp_path / "silver" / "stocks"

    stock_dir = bronze_path / "NVDA"
    stock_dir.mkdir(parents=True)

    create_test_dataframe([
        "2020-01-03",
        "2020-01-04",
    ]).write_parquet(stock_dir / "NVDA_batch1_raw.pq")

    create_test_dataframe([
        "2020-01-01",
        "2020-01-02",
    ]).write_parquet(stock_dir / "NVDA_batch2_raw.pq")

    monkeypatch.setattr(stocks, "HISTORICAL_STOCKS_DATA_PATH", bronze_path)
    monkeypatch.setattr(stocks, "HISTORICAL_SILVER_STOCKS_PATH", silver_path)

    result = stocks.transform_historical_stocks(["NVDA"])

    assert result is True

    output = pl.read_parquet(silver_path / "NVDA_silver.pq")

    assert output.height == 4
    assert output["date"].is_sorted()


def test_duplicate_dates_are_removed(tmp_path, monkeypatch):
    bronze_path = tmp_path / "historical" / "stocks"
    silver_path = tmp_path / "silver" / "stocks"

    stock_dir = bronze_path / "NVDA"
    stock_dir.mkdir(parents=True)

    create_test_dataframe([
        "2020-01-01",
        "2020-01-02",
    ]).write_parquet(stock_dir / "NVDA_batch1_raw.pq")

    create_test_dataframe([
        "2020-01-02",
        "2020-01-03",
    ]).write_parquet(stock_dir / "NVDA_batch2_raw.pq")

    monkeypatch.setattr(stocks, "HISTORICAL_STOCKS_DATA_PATH", bronze_path)
    monkeypatch.setattr(stocks, "HISTORICAL_SILVER_STOCKS_PATH", silver_path)

    result = stocks.transform_historical_stocks(["NVDA"])

    assert result is True

    output = pl.read_parquet(silver_path / "NVDA_silver.pq")

    assert output.height == 3
    assert output["date"].n_unique() == 3


def test_missing_bronze_data_returns_false(tmp_path, monkeypatch):
    bronze_path = tmp_path / "historical" / "stocks"
    silver_path = tmp_path / "silver" / "stocks"

    bronze_path.mkdir(parents=True)

    monkeypatch.setattr(stocks, "HISTORICAL_STOCKS_DATA_PATH", bronze_path)
    monkeypatch.setattr(stocks, "HISTORICAL_SILVER_STOCKS_PATH", silver_path)

    result = stocks.transform_historical_stocks(["NVDA"])

    assert result is False
    assert not (silver_path / "NVDA_silver.pq").exists()

def test_invalid_bronze_data_is_not_saved(tmp_path, monkeypatch):
    bronze_path = tmp_path / "historical" / "stocks"
    silver_path = tmp_path / "silver" / "stocks"

    stock_dir = bronze_path / "NVDA"
    stock_dir.mkdir(parents=True)

    df = create_test_dataframe([
        "2020-01-01",
        "2020-01-02",
    ])

    # Introduce invalid OHLC data.
    df = df.with_columns(
        pl.lit(80.0).alias("high")
    )

    df.write_parquet(stock_dir / "NVDA_batch1_raw.pq")

    monkeypatch.setattr(stocks, "HISTORICAL_STOCKS_DATA_PATH", bronze_path)
    monkeypatch.setattr(stocks, "HISTORICAL_SILVER_STOCKS_PATH", silver_path)

    result = stocks.transform_historical_stocks(["NVDA"])

    assert result is False
    assert not (silver_path / "NVDA_silver.pq").exists()
