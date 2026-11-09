
import pytest

from market_data.storage import (
    LocalStorage,
    S3Storage,
    get_storage,
)


def test_factory_defaults_to_local(monkeypatch, tmp_path):
    monkeypatch.delenv("MARKET_DATA_STORAGE_BACKEND", raising=False)

    storage = get_storage(root=tmp_path)

    assert isinstance(storage, LocalStorage)


def test_factory_selects_local(monkeypatch, tmp_path):
    monkeypatch.setenv("MARKET_DATA_STORAGE_BACKEND", "local")

    storage = get_storage(root=tmp_path)

    assert isinstance(storage, LocalStorage)


def test_factory_rejects_unknown_backend():
    with pytest.raises(ValueError):
        get_storage(backend="unknown")


def test_factory_requires_s3_bucket(monkeypatch):
    monkeypatch.delenv("MARKET_DATA_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="MARKET_DATA_S3_BUCKET"):
        get_storage(backend="s3")


def test_factory_selects_s3(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_S3_BUCKET", "market-data-test")
    monkeypatch.setenv("MARKET_DATA_S3_PREFIX", "development")

    # Dummy credentials: no real AWS authentication is needed.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")

    storage = get_storage(backend="s3")

    assert isinstance(storage, S3Storage)
    assert storage.bucket == "market-data-test"
    assert storage.prefix == "development"