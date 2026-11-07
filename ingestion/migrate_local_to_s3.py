"""One-off: copy every day already landed in the local folder into the S3 store.

Safe to re-run: each day is written with the same idempotent write_partition.

    python -m ingestion.migrate_local_to_s3
"""

from __future__ import annotations

import logging

from ingestion.config import get_settings, get_store
from ingestion.storage import LocalStore, S3Store

log = logging.getLogger("migrate")


def migrate(local: LocalStore, target: S3Store) -> int:
    days = local.list_partitions()
    for dt in days:
        files = dict(local.read_partition(dt))
        target.write_partition(dt, files)
        log.info("copied %s: %d files -> %s", dt, len(files), target.describe(dt))
    return len(days)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = get_settings()
    target = get_store(s)
    if not isinstance(target, S3Store):
        raise SystemExit("Set STORAGE_BACKEND=s3 and S3_BUCKET in .env first.")
    n = migrate(LocalStore(s.data_dir), target)
    log.info("done: %d day(s) copied", n)


if __name__ == "__main__":
    main()
