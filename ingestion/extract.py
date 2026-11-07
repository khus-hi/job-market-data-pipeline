"""Extract: call the API and land the responses, untouched, as raw JSON files.

Layout (the same layout moves to S3 in Stage 2):
    data/raw/postings/dt=YYYY-MM-DD/<role>_page_NNN.json

Idempotency: a run for a date first writes into a temporary folder, then swaps
it in place of any existing folder for that date. Re-running a day replaces
that day's files; it never appends a second copy.
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

from ingestion.adzuna_client import PostingSource
from ingestion.config import ROLE_QUERIES

log = logging.getLogger(__name__)


def partition_dir(raw_dir: Path, dt: date) -> Path:
    return raw_dir / f"dt={dt.isoformat()}"


def extract_day(
    source: PostingSource,
    dt: date,
    raw_dir: Path,
    max_pages: int,
    roles: list[str] = ROLE_QUERIES,
) -> dict[str, int]:
    """Fetch every role query for one snapshot date. Returns counts for logging."""
    final_dir = partition_dir(raw_dir, dt)
    tmp_dir = final_dir.with_name(final_dir.name + ".tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)  # leftovers from a crashed run
    tmp_dir.mkdir(parents=True)

    ingested_at = datetime.now(UTC).isoformat()
    files = postings = 0
    for role in roles:
        for page in range(1, max_pages + 1):
            response = source.search(role, page)
            results = response.get("results", [])
            if not results:
                break  # no more pages for this role
            envelope = {
                # Metadata about the fetch, kept next to the untouched API payload.
                "snapshot_date": dt.isoformat(),
                "ingested_at": ingested_at,
                "search_role": role,
                "page": page,
                "response": response,
            }
            out = tmp_dir / f"{role.replace(' ', '_')}_page_{page:03d}.json"
            out.write_text(json.dumps(envelope), encoding="utf-8")
            files += 1
            postings += len(results)
            log.info("fetched role=%r page=%d postings=%d", role, page, len(results))
            if len(results) < getattr(source, "results_per_page", 50):
                break  # short page means it was the last one

    # Swap: remove the old partition, then rename tmp into place.
    if final_dir.exists():
        shutil.rmtree(final_dir)
    tmp_dir.rename(final_dir)
    log.info("landed %d files / %d postings in %s", files, postings, final_dir)
    return {"files": files, "postings": postings}
