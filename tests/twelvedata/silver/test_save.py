
from datetime import datetime

import polars as pl
import pytest

from market_data.storage.local import LocalStorage
from market_data.pipelines.twelvedata.silver.save import (
    save_stock_silver,
)


@pytest.fixture
def silver_df():
    return pl.DataFrame({
        "asset_name": ["NVDA", "NVDA"],
        "date": [
            datetime(2026, 11, 5),
            datetime(2026, 11, 6),
        ],
        "close": [105.0, 110.0],
    })


def test_save_silver_with_storage(silver_df, tmp_path):
    storage = LocalStorage(tmp_path / "storage")
    key = "NVDA/NVDA_silver.pq"

    result = save_stock_silver(
        df=silver_df,
        output_path=tmp_path / "unused.pq",
        storage=storage,
        storage_key=key,
    )

    assert result == key
    assert storage.exists(key)
    assert storage.read_parquet(key).equals(silver_df)


def test_save_silver_requires_storage_key(silver_df, tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    with pytest.raises(ValueError, match="storage_key"):
        save_stock_silver(
            df=silver_df,
            output_path=tmp_path / "unused.pq",
            storage=storage,
        )


def test_save_silver_rejects_empty_dataframe(tmp_path):
    storage = LocalStorage(tmp_path / "storage")

    with pytest.raises(ValueError, match="empty"):
        save_stock_silver(
            df=pl.DataFrame(),
            output_path=tmp_path / "unused.pq",
            storage=storage,
            storage_key="NVDA/NVDA_silver.pq",
        )
