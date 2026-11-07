import pytest

from ingestion.config import get_settings, get_store
from ingestion.storage import LocalStore, S3Store


def settings_with(monkeypatch, **env):
    for key in ("STORAGE_BACKEND", "S3_BUCKET", "S3_ENDPOINT_URL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("ingestion.config.load_dotenv", lambda *a, **k: None)  # ignore real .env
    return get_settings()


def test_local_is_the_default(monkeypatch):
    assert isinstance(get_store(settings_with(monkeypatch)), LocalStore)


def test_s3_backend_requires_bucket(monkeypatch):
    with pytest.raises(ValueError, match="S3_BUCKET"):
        get_store(settings_with(monkeypatch, STORAGE_BACKEND="s3"))


def test_s3_backend_with_minio_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    s = settings_with(
        monkeypatch, STORAGE_BACKEND="S3", S3_BUCKET="b", S3_ENDPOINT_URL="http://localhost:9000"
    )
    store = get_store(s)
    assert isinstance(store, S3Store)
    assert store.client.meta.endpoint_url == "http://localhost:9000"


def test_unknown_backend_is_rejected(monkeypatch):
    with pytest.raises(ValueError, match="Unknown STORAGE_BACKEND"):
        get_store(settings_with(monkeypatch, STORAGE_BACKEND="gcs"))
