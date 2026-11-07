"""Central configuration. Every setting comes from environment variables (.env),
so no secret or machine-specific path is ever hard-coded."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Repo root = the folder that contains this package.
REPO_ROOT = Path(__file__).resolve().parent.parent

# The role searches we run against Adzuna. The label is only a search hint;
# the real role used in analytics is classified from the job title in dbt.
ROLE_QUERIES: list[str] = [
    "data engineer",
    "data analyst",
    "analytics engineer",
    "data scientist",
]


@dataclass(frozen=True)
class Settings:
    adzuna_app_id: str | None
    adzuna_app_key: str | None
    data_dir: Path
    duckdb_path: Path
    max_pages: int
    max_days_old: int
    storage_backend: str = "local"  # local | s3  (MinIO is s3 with an endpoint URL)
    s3_bucket: str | None = None
    s3_endpoint_url: str | None = None  # set for MinIO, leave empty for AWS
    aws_region: str = "us-east-1"
    results_per_page: int = 50  # Adzuna's maximum

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw" / "postings"


def _resolve(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else REPO_ROOT / path


def get_settings() -> Settings:
    load_dotenv(REPO_ROOT / ".env")
    return Settings(
        adzuna_app_id=os.getenv("ADZUNA_APP_ID"),
        adzuna_app_key=os.getenv("ADZUNA_APP_KEY"),
        data_dir=_resolve(os.getenv("DATA_DIR", "data")),
        duckdb_path=_resolve(os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")),
        max_pages=int(os.getenv("ADZUNA_MAX_PAGES", "3")),
        max_days_old=int(os.getenv("ADZUNA_MAX_DAYS_OLD", "2")),
        storage_backend=os.getenv("STORAGE_BACKEND", "local").strip().lower(),
        s3_bucket=os.getenv("S3_BUCKET") or None,
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
    )


def get_store(settings: Settings):
    """Build the raw landing-zone backend chosen in .env."""
    from ingestion.storage import LocalStore, S3Store

    if settings.storage_backend == "local":
        return LocalStore(settings.data_dir)
    if settings.storage_backend == "s3":
        if not settings.s3_bucket:
            raise ValueError("STORAGE_BACKEND=s3 needs S3_BUCKET in .env")
        return S3Store.from_settings(
            settings.s3_bucket, settings.s3_endpoint_url, settings.aws_region
        )
    raise ValueError(f"Unknown STORAGE_BACKEND={settings.storage_backend!r} (use local or s3)")
