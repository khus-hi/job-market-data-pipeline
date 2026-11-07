"""Skill extraction: find known skills mentioned in a posting's title + description.

The skill list lives in ONE place, dbt/seeds/skill_catalog.csv. This module
reads it for extraction, and dbt loads the same file as the dim_skill table.
Adding a skill = adding one CSV row.

Approach: a dictionary of regex patterns with word boundaries. It is simple,
fast, explainable and testable. Its known limit: it only finds skills we listed,
and Adzuna truncates descriptions (~500 chars), so counts are a lower bound.
"""

from __future__ import annotations

import csv
import html
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from ingestion.config import REPO_ROOT

CATALOG_PATH = REPO_ROOT / "dbt" / "seeds" / "skill_catalog.csv"
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class Skill:
    name: str
    category: str
    regex: re.Pattern[str]


@lru_cache(maxsize=4)
def load_catalog(path: Path = CATALOG_PATH) -> tuple[Skill, ...]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    skills = []
    for row in rows:
        flags = 0 if row["case_sensitive"].strip().lower() == "true" else re.IGNORECASE
        skills.append(Skill(row["skill_name"], row["category"], re.compile(row["pattern"], flags)))
    return tuple(skills)


def clean_text(text: str | None) -> str:
    """Strip HTML tags and entities, collapse whitespace."""
    if not text:
        return ""
    text = html.unescape(_TAG_RE.sub(" ", text))
    return re.sub(r"\s+", " ", text).strip()


def extract_skills(*texts: str | None, catalog: tuple[Skill, ...] | None = None) -> list[str]:
    """Return the sorted, de-duplicated skill names found in any of the texts."""
    catalog = catalog or load_catalog()
    blob = " ".join(clean_text(t) for t in texts)
    return sorted({s.name for s in catalog if s.regex.search(blob)})
