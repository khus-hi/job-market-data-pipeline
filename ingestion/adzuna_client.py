"""Thin HTTP client for the Adzuna job search API.

Docs: https://developer.adzuna.com/docs/search
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search/{page}"


class AdzunaError(RuntimeError):
    """Raised on API failures. The message never contains the API key."""


class PostingSource(Protocol):
    """Anything that can return one page of search results.
    Both the real client and the sample generator follow this shape, so the
    extractor (and the tests) don't care which one they get."""

    def search(self, role: str, page: int) -> dict[str, Any]: ...


class AdzunaClient:
    def __init__(
        self,
        app_id: str,
        app_key: str,
        results_per_page: int = 50,
        max_days_old: int = 2,
        min_seconds_between_calls: float = 2.5,  # free tier is ~25 calls/minute
        session: requests.Session | None = None,
    ) -> None:
        if not app_id or not app_key:
            raise AdzunaError(
                "ADZUNA_APP_ID / ADZUNA_APP_KEY are missing. Copy .env.example to .env "
                "and fill them in, or run with --source sample."
            )
        self._auth = {"app_id": app_id, "app_key": app_key}
        self.results_per_page = results_per_page
        self.max_days_old = max_days_old
        self.min_interval = min_seconds_between_calls
        self._last_call = 0.0
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        # Retry transient failures (rate limit, server errors) with exponential backoff.
        retry = Retry(
            total=4,
            backoff_factor=2,  # waits 2s, 4s, 8s, 16s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def search(self, role: str, page: int) -> dict[str, Any]:
        params = {
            **self._auth,
            "results_per_page": self.results_per_page,
            "title_only": role,
            "max_days_old": self.max_days_old,
            "sort_by": "date",
            "content-type": "application/json",
        }
        self._throttle()
        try:
            resp = self.session.get(BASE_URL.format(page=page), params=params, timeout=30)
        except requests.RequestException as exc:
            # str(exc) can include the full URL with the key, so we don't re-raise it as-is.
            raise AdzunaError(f"Network error calling Adzuna ({type(exc).__name__})") from None
        if resp.status_code != 200:
            raise AdzunaError(f"Adzuna returned HTTP {resp.status_code} for '{role}' page {page}")
        return resp.json()
