"""Extract: call the API and land the responses, untouched, as raw JSON files.

Layout, the same in every backend (local folder, S3, MinIO):
    raw/postings/dt=YYYY-MM-DD/<role>_page_NNN.json

All pages are fetched into memory first and written in one go at the end, so a
failed API call never leaves a half-written day behind. The store then replaces
that day's files, so re-running a day never appends a second copy.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime

from ingestion.adzuna_client import PostingSource
from ingestion.config import ROLE_QUERIES
from ingestion.storage import RawStore

log = logging.getLogger(__name__)


def extract_day(
    source: PostingSource,
    dt: date,
    store: RawStore,
    max_pages: int,
    roles: list[str] = ROLE_QUERIES,
) -> dict[str, int]:
    """Fetch every role query for one snapshot date. Returns counts for logging."""
    ingested_at = datetime.now(UTC).isoformat()
    files: dict[str, bytes] = {}
    postings = 0
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
            name = f"{role.replace(' ', '_')}_page_{page:03d}.json"
            files[name] = json.dumps(envelope).encode("utf-8")
            postings += len(results)
            log.info("fetched role=%r page=%d postings=%d", role, page, len(results))
            if len(results) < getattr(source, "results_per_page", 50):
                break  # short page means it was the last one

    store.write_partition(dt, files)
    log.info("landed %d files / %d postings in %s", len(files), postings, store.describe(dt))
    return {"files": len(files), "postings": postings}
