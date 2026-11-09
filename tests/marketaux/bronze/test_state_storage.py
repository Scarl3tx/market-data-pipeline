
from market_data.pipelines.marketaux.bronze.state import (
    MarketauxNewsState,
    S3_STATE_KEY,
)
from market_data.storage.local import LocalStorage


def test_storage_backend_uses_default_s3_key(tmp_path):
    storage = LocalStorage(tmp_path)

    state = MarketauxNewsState(storage=storage)

    assert state.state_path == S3_STATE_KEY
    assert state.state == {"stocks": {}}


def test_storage_backend_persists_state_between_instances(tmp_path):
    storage = LocalStorage(tmp_path)

    state = MarketauxNewsState(storage=storage)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])
    state.record_attempt("NVDA")

    reloaded = MarketauxNewsState(storage=storage)

    assert reloaded.get_stock("NVDA")["last_attempt_at"] is not None


def test_storage_backend_resumes_ingestion_checkpoint(tmp_path):
    storage = LocalStorage(tmp_path)

    state = MarketauxNewsState(storage=storage)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.start_ingestion("NVDA")
    state.record_page_saved("NVDA", 1)

    reloaded = MarketauxNewsState(storage=storage)

    stock = reloaded.get_stock("NVDA")

    assert stock["ingestion_in_progress"] is True
    assert stock["next_page"] == 2
    assert stock["pending_since"] is not None
    assert stock["pending_until"] is not None


def test_storage_backend_accepts_explicit_key(tmp_path):
    storage = LocalStorage(tmp_path)
    key = "state/custom/marketaux.json"

    state = MarketauxNewsState(
        state_path=key,
        storage=storage,
    )
    state.initialize_stocks([
        {"symbol": "AAPL", "name": "Apple"}
    ])

    assert storage.exists(key)
    assert state.get_stock("AAPL")["name"] == "Apple"


def test_storage_backend_preserves_backfill_progress(tmp_path):
    storage = LocalStorage(tmp_path)

    state = MarketauxNewsState(storage=storage)
    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.start_ingestion("NVDA")
    state.record_page_saved("NVDA", 1)
    state.complete_ingestion("NVDA")

    state.start_backfill("NVDA", window_days=30)
    state.record_page_saved("NVDA", 1)

    reloaded = MarketauxNewsState(storage=storage)
    stock = reloaded.get_stock("NVDA")

    assert stock["ingestion_type"] == "backfill"
    assert stock["next_page"] == 2
    assert stock["ingestion_in_progress"] is True
