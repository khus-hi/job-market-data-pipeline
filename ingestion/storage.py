"""Raw landing zone storage: a local folder, AWS S3, or any S3-compatible store (MinIO).

Every backend uses the same Hive-style layout:

    <root>/raw/postings/dt=YYYY-MM-DD/<role>_page_NNN.json

`extract.py` and `load.py` only talk to the `RawStore` interface, so switching from a
laptop folder to S3 is a config change (STORAGE_BACKEND), not a code change.
"""

from __future__ import annotations

import logging
import shutil
from datetime import date
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)

RAW_PREFIX = "raw/postings"


def partition_name(dt: date) -> str:
    return f"dt={dt.isoformat()}"


class RawStore(Protocol):
    def write_partition(self, dt: date, files: dict[str, bytes]) -> None:
        """Replace everything stored for `dt` with exactly `files` (idempotent)."""

    def read_partition(self, dt: date) -> list[tuple[str, bytes]]:
        """Return (file name, content) for every file stored for `dt`, sorted by name."""

    def list_partitions(self) -> list[date]:
        """Dates that have data, oldest first."""

    def describe(self, dt: date) -> str:
        """Human-readable location, for logs."""


class LocalStore:
    """Folder on disk. Writes go to a temp folder that is then swapped into place."""

    def __init__(self, root: Path) -> None:
        self.base = root / RAW_PREFIX

    def _dir(self, dt: date) -> Path:
        return self.base / partition_name(dt)

    def describe(self, dt: date) -> str:
        return str(self._dir(dt))

    def write_partition(self, dt: date, files: dict[str, bytes]) -> None:
        final_dir = self._dir(dt)
        tmp_dir = final_dir.with_name(final_dir.name + ".tmp")
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)  # leftovers from a crashed run
        tmp_dir.mkdir(parents=True)
        for name, body in files.items():
            (tmp_dir / name).write_bytes(body)
        if final_dir.exists():
            shutil.rmtree(final_dir)
        tmp_dir.rename(final_dir)

    def read_partition(self, dt: date) -> list[tuple[str, bytes]]:
        folder = self._dir(dt)
        if not folder.exists():
            raise FileNotFoundError(f"No raw data for {dt} at {folder}. Run extract first.")
        return [(p.name, p.read_bytes()) for p in sorted(folder.glob("*.json"))]

    def list_partitions(self) -> list[date]:
        if not self.base.exists():
            return []
        return sorted(
            date.fromisoformat(p.name.removeprefix("dt="))
            for p in self.base.glob("dt=*")
            if p.is_dir() and not p.name.endswith(".tmp")
        )


class S3Store:
    """S3 bucket (AWS or MinIO). S3 has no folders and no atomic rename, so
    idempotency works differently from LocalStore:

    1. upload every new file (same name = overwrite, so re-runs don't duplicate)
    2. delete any old key in the partition that wasn't part of this run

    A reader during those few seconds could see a mix of old and new files for one
    day. That's acceptable here because the loader runs after extract finishes.
    """

    def __init__(self, bucket: str, client) -> None:
        self.bucket = bucket
        self.client = client

    @classmethod
    def from_settings(cls, bucket: str, endpoint_url: str | None, region: str) -> S3Store:
        import boto3  # imported here so local-only runs don't need AWS configured

        # Credentials come from the standard AWS chain: env vars from .env
        # (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), or ~/.aws/credentials.
        client = boto3.client("s3", endpoint_url=endpoint_url or None, region_name=region)
        return cls(bucket, client)

    def _prefix(self, dt: date) -> str:
        return f"{RAW_PREFIX}/{partition_name(dt)}/"

    def describe(self, dt: date) -> str:
        return f"s3://{self.bucket}/{self._prefix(dt)}"

    def _keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def write_partition(self, dt: date, files: dict[str, bytes]) -> None:
        prefix = self._prefix(dt)
        new_keys = {prefix + name for name in files}
        for name, body in files.items():
            self.client.put_object(
                Bucket=self.bucket, Key=prefix + name, Body=body, ContentType="application/json"
            )
        stale = [k for k in self._keys(prefix) if k not in new_keys]
        for i in range(0, len(stale), 1000):  # delete_objects takes at most 1000 keys
            batch = [{"Key": k} for k in stale[i : i + 1000]]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
        if stale:
            log.info("removed %d stale objects from %s", len(stale), self.describe(dt))

    def read_partition(self, dt: date) -> list[tuple[str, bytes]]:
        prefix = self._prefix(dt)
        keys = sorted(k for k in self._keys(prefix) if k.endswith(".json"))
        if not keys:
            raise FileNotFoundError(
                f"No raw data for {dt} at {self.describe(dt)}. Run extract first."
            )
        out = []
        for key in keys:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            out.append((key.removeprefix(prefix), body))
        return out

    def list_partitions(self) -> list[date]:
        resp_pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=f"{RAW_PREFIX}/", Delimiter="/"
        )
        dates = []
        for page in resp_pages:
            for cp in page.get("CommonPrefixes", []):
                part = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                dates.append(date.fromisoformat(part.removeprefix("dt=")))
        return sorted(dates)
