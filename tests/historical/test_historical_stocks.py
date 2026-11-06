
import polars as pl
from datetime import datetime
from src.historical.ingestion import stocks


def test_existing_historical_batch_is_skipped(tmp_path, monkeypatch):

    # Redirect historical output to a temporary directory
    monkeypatch.setattr(
        stocks,
        "HISTORICAL_STOCKS_DATA_PATH",
        tmp_path
    )

    symbol = "NVDA"

    # Create a dummy existing batch
    output_dir = tmp_path / symbol
    output_dir.mkdir(parents=True)

    output_path = (
        output_dir / "NVDA_20261001_20261102_raw.pq"
    )

    existing_df = pl.DataFrame({"original": [123]})
    existing_df.write_parquet(output_path)

    # Mock provider returning the same date range
    class MockProvider:
        def get_stock_history(self, symbol, start_date, end_date):
            return pl.DataFrame({
                "asset_name": ["NVDA", "NVDA"],
                "date": [
                    datetime(2026, 10, 1),
                    datetime(2026, 11, 2)
                ],
                "open": [100.0, 105.0],
                "high": [110.0, 115.0],
                "low": [95.0, 100.0],
                "close": [105.0, 110.0],
                "volume": [1000, 1200]
            })
