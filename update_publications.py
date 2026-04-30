#!/usr/bin/env python3
"""
Fetch Michael L. Platt publications from Crossref and write publications.json.

Default behavior:
- Uses Michael L. Platt's ORCID as the strongest match signal.
- Falls back to name variants because academic metadata is a haunted filing cabinet.
- Deduplicates by DOI.
- Applies neuroscience/affiliation scoring to reduce false positives.
- Supports manual DOI allowlist/blocklist files.

Required environment variable:
- CROSSREF_MAILTO: your email address for Crossref polite API access.

Optional environment variables:
- TARGET_AUTHOR: default "Michael L. Platt"
- TARGET_ORCID: default "0000-0003-3912-8821"
- OUTPUT_FILE: default "publications.json"
- ROWS_PER_PAGE: default "100"
- MAX_PAGES_PER_QUERY: default "3"
- MIN_NAME_FALLBACK_SCORE: default "130"
- WRITE_DEBUG: "1" to also write publications.debug.json
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests


# -------------------------
# Configuration
# -------------------------

TARGET_AUTHOR = os.getenv("TARGET_AUTHOR", "Michael L. Platt").strip()
TARGET_ORCID = os.getenv("TARGET_ORCID", "0000-0003-3912-8821").strip()
CROSSREF_MAILTO = os.getenv("CROSSREF_MAILTO", "").strip()

OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", "publications.json"))
DEBUG_OUTPUT_FILE = Path(os.getenv("DEBUG_OUTPUT_FILE", "publications.debug.json"))

ROWS_PER_PAGE = int(os.getenv("ROWS_PER_PAGE", "100"))
MAX_PAGES_PER_QUERY = int(os.getenv("MAX_PAGES_PER_QUERY", "3"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
MIN_NAME_FALLBACK_SCORE = int(os.getenv("MIN_NAME_FALLBACK_SCORE", "130"))

AUTHOR_VARIANTS = [
    TARGET_AUTHOR,
    "Michael Louis Platt",
    "Michael Platt",
    "Michael L Platt",
    "M L Platt",
    "M. L. Platt",
    "ML Platt",
]

NEUROSCIENCE_KEYWORDS = {
    "amygdala",
    "attention",
    "behavior",
    "behaviour",
    "brain",
    "cognition",
    "cognitive",
    "cortex",
    "cortical",
    "decision",
    "dopamine",
    "electrophysiology",
    "evolution",
    "fmri",
    "hippocampus",
    "learning",
    "macaque",
    "monkey",
    "motivation",
    "neural",
    "neuro",
    "neurobiology",
    "neuroeconomics",
    "neuron",
    "neuronal",
    "neuroscience",
    "prefrontal",
    "primate",
    "psychology",
    "reward",
    "social",
    "striatum",
}

AFFILIATION_KEYWORDS = {
    "university of pennsylvania",
    "penn medicine",
    "perelman",
    "wharton",
    "duke",
    "center for cognitive neuroscience",
    "institute for brain sciences",
    "platt lab",
}

LOW_RELEVANCE_KEYWORDS = {
    "agriculture",
    "astronomy",
    "botany",
    "civil engineering",
    "geology",
    "mechanical engineering",
}

SUPPORTED_TYPES = {
    "journal-article",
    "posted-content",
    "proceedings-article",
    "book-chapter",
    "reference-entry",
    "monograph",
}

CROSSREF_WORKS_URL = "https://api.crossref.org/works"


# -------------------------
# Basic utilities
# -------------------------

def die(message: str, code: int = 1) -> int:
    print(f"ERROR: {message}", file=sys.stderr)
    return code


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_doi(value: str) -> str:
    doi = str(value or "").strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    return doi.lower()


def safe_first(value: Any, default: str = "") -> str:
    if isinstance(value, list) and value:
        return str(value[0]).strip()
    if isinstance(value, str):
        return value.strip()
    return default


def strip_jats(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value or "")


def read_doi_list(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    values: Set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        clean = line.split("#", 1)[0].strip()
        if clean:
            values.add(normalize_doi(clean))
    return values


def unique_keep_order(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []
    for value in values:
        clean = normalize_whitespace(value)
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            result.append(clean)
    return result


# -------------------------
# Crossref parsing
# -------------------------

def parse_date_parts(item: Dict[str, Any]) -> Tuple[Optional[int], str]:
    for key in ("published-print", "published-online", "published", "issued", "created", "deposited"):
        date_parts = item.get(key, {}).get("date-parts")
        if not date_parts or not isinstance(date_parts, list) or not date_parts[0]:
            continue

        parts = date_parts[0]
        try:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1
            return year, f"{year:04d}-{month:02d}-{day:02d}"
        except (TypeError, ValueError, IndexError):
            continue

    return None, ""


def extract_orcid(value: Any) -> str:
    raw = str(value or "")
    match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", raw, re.IGNORECASE)
    return match.group(1).upper() if match else ""


def format_author(author: Dict[str, Any]) -> Dict[str, str]:
    given = normalize_whitespace(str(author.get("given", "")))
    family = normalize_whitespace(str(author.get("family", "")))
    literal = normalize_whitespace(str(author.get("literal", "")))
    orcid = extract_orcid(author.get("ORCID"))

    if given or family:
        name = normalize_whitespace(f"{given} {family}")
    else:
        name = literal

    return {
        "name": name,
        "given": given,
        "family": family,
        "orcid": orcid,
    }


def extract_authors(item: Dict[str, Any]) -> List[Dict[str, str]]:
    raw_authors = item.get("author")
    if not isinstance(raw_authors, list):
        return []

    authors = []
    for author in raw_authors:
        if not isinstance(author, dict):
            continue
        parsed = format_author(author)
        if parsed["name"]:
            authors.append(parsed)

    return authors


def author_name_matches_target(author: Dict[str, str]) -> bool:
    family = re.sub(r"[^a-z]", "", author.get("family", "").lower())
    given = author.get("given", "").lower()
    name = author.get("name", "").lower()

    compact_given = re.sub(r"[^a-z]", "", given)
    normalized_name = normalize_whitespace(re.sub(r"[^a-z0-9]+", " ", name))

    if family == "platt" and compact_given in {
        "michael",
        "michaell",
        "michaellouis",
        "ml",
        "m",
    }:
        return True

    patterns = [
        r"\bmichael\s+louis\s+platt\b",
        r"\bmichael\s+l\s+platt\b",
        r"\bmichael\s+platt\b",
        r"\bm\s+l\s+platt\b",
        r"\bml\s+platt\b",
    ]
    return any(re.search(pattern, normalized_name) for pattern in patterns)


def item_text_blob(item: Dict[str, Any], authors: List[Dict[str, str]]) -> str:
    fields: List[str] = [
        safe_first(item.get("title")),
        safe_first(item.get("subtitle")),
        safe_first(item.get("container-title")),
        safe_first(item.get("short-container-title")),
        str(item.get("publisher", "")),
        str(item.get("type", "")),
    ]

    abstract = item.get("abstract")
    if isinstance(abstract, str):
        fields.append(strip_jats(abstract))

    subjects = item.get("subject")
    if isinstance(subjects, list):
        fields.extend(str(subject) for subject in subjects)

    for author in authors:
        fields.append(author.get("name", ""))
        raw_affiliations = item.get("affiliation")
        if isinstance(raw_affiliations, list):
            fields.extend(str(a.get("name", "")) for a in raw_affiliations if isinstance(a, dict))

    # Crossref affiliations are often nested per author.
    for raw_author in item.get("author", []) if isinstance(item.get("author"), list) else []:
        if not isinstance(raw_author, dict):
            continue
        affiliations = raw_author.get("affiliation", [])
        if isinstance(affiliations, list):
            fields.extend(str(a.get("name", "")) for a in affiliations if isinstance(a, dict))

    combined = " ".join(fields).lower()
    return normalize_whitespace(combined)


def score_item(item: Dict[str, Any]) -> Tuple[int, List[str], Dict[str, int]]:
    authors = extract_authors(item)
    text_blob = item_text_blob(item, authors)
    item_type = str(item.get("type", "")).lower()

    score = 0
    reasons: List[str] = []

    has_orcid_match = any(author.get("orcid") == TARGET_ORCID.upper() for author in authors)
    has_name_match = any(author_name_matches_target(author) for author in authors)

    if has_orcid_match:
        score += 200
        reasons.append("orcid_match")

    if has_name_match:
        score += 120
        reasons.append("name_match")

    neuro_hits = sum(1 for kw in NEUROSCIENCE_KEYWORDS if kw in text_blob)
    affiliation_hits = sum(1 for kw in AFFILIATION_KEYWORDS if kw in text_blob)
    low_relevance_hits = sum(1 for kw in LOW_RELEVANCE_KEYWORDS if kw in text_blob)

    if neuro_hits:
        score += min(neuro_hits * 5, 60)
        reasons.append(f"neuroscience_context:{neuro_hits}")

    if affiliation_hits:
        score += min(affiliation_hits * 10, 40)
        reasons.append(f"affiliation_context:{affiliation_hits}")

    if low_relevance_hits:
        score -= min(low_relevance_hits * 12, 36)
        reasons.append(f"low_relevance_context:{low_relevance_hits}")

    if item_type in SUPPORTED_TYPES:
        score += 8
        reasons.append(f"type:{item_type}")

    journal = safe_first(item.get("container-title")).lower()
    if any(hint in journal for hint in ("neuro", "brain", "cognition", "behavior", "behaviour", "psycholog", "pnas", "nature", "science")):
        score += 10
        reasons.append("journal_hint")

    year, _ = parse_date_parts(item)
    if year:
        # Tiny recency boost. It should break ties, not rewrite reality.
        score += max(0, min(year - 1990, 30)) // 3
        reasons.append(f"year:{year}")

    diagnostics = {
        "has_orcid_match": int(has_orcid_match),
        "has_name_match": int(has_name_match),
        "neuro_hits": neuro_hits,
        "affiliation_hits": affiliation_hits,
        "low_relevance_hits": low_relevance_hits,
    }
    return score, reasons, diagnostics


def include_item(
    item: Dict[str, Any],
    score: int,
    diagnostics: Dict[str, int],
    allowlist: Set[str],
    blocklist: Set[str],
) -> bool:
    doi = normalize_doi(item.get("DOI", ""))

    if doi and doi in blocklist:
        return False

    if doi and doi in allowlist:
        return True

    if diagnostics["has_orcid_match"]:
        return True

    if not diagnostics["has_name_match"]:
        return False

    has_context = (
        diagnostics["neuro_hits"] > 0
        or diagnostics["affiliation_hits"] > 0
    )

    return bool(has_context and score >= MIN_NAME_FALLBACK_SCORE)


def build_record(item: Dict[str, Any], score: int, reasons: List[str]) -> Dict[str, Any]:
    authors = extract_authors(item)
    author_names = [author["name"] for author in authors if author.get("name")]
    year, published_date = parse_date_parts(item)
    doi = normalize_doi(item.get("DOI", ""))

    url = f"https://doi.org/{doi}" if doi else str(item.get("URL", "")).strip()

    return {
        "title": safe_first(item.get("title"), "Untitled"),
        "authors": unique_keep_order(author_names),
        "authors_display": ", ".join(unique_keep_order(author_names)),
        "journal": safe_first(item.get("container-title")),
        "publisher": str(item.get("publisher", "")).strip(),
        "year": year,
        "published_date": published_date,
        "doi": doi,
        "url": url,
        "type": str(item.get("type", "")).strip(),
        "score": score,
        "match_reasons": reasons,
    }


# -------------------------
# Crossref fetching
# -------------------------

def crossref_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": f"squarespace-publications-bot/1.0 (mailto:{CROSSREF_MAILTO})",
    })
    return session


def fetch_crossref_pages(
    session: requests.Session,
    *,
    query_author: Optional[str] = None,
    orcid: Optional[str] = None,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    cursor = "*"
    last_cursor = ""

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        params: Dict[str, Any] = {
            "rows": ROWS_PER_PAGE,
            "cursor": cursor,
            "sort": "published",
            "order": "desc",
            "mailto": CROSSREF_MAILTO,
        }

        if query_author:
            params["query.author"] = query_author

        if orcid:
            params["filter"] = f"orcid:{orcid}"

        response = session.get(CROSSREF_WORKS_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        message = response.json().get("message", {})
        page_items = message.get("items", [])
        if not isinstance(page_items, list) or not page_items:
            break

        items.extend(page_items)

        next_cursor = str(message.get("next-cursor", ""))
        if not next_cursor or next_cursor == last_cursor:
            break

        last_cursor = cursor
        cursor = next_cursor

        # Polite pause. Tiny, but better than acting like a broken scraper goblin.
        time.sleep(0.5)

    return items


def collect_candidates() -> List[Dict[str, Any]]:
    session = crossref_session()
    all_items: List[Dict[str, Any]] = []

    if TARGET_ORCID:
        print(f"Fetching Crossref records by ORCID: {TARGET_ORCID}")
        all_items.extend(fetch_crossref_pages(session, orcid=TARGET_ORCID))

    for variant in unique_keep_order(AUTHOR_VARIANTS):
        print(f"Fetching Crossref records by author query: {variant}")
        all_items.extend(fetch_crossref_pages(session, query_author=variant))
        time.sleep(0.5)

    return all_items


# -------------------------
# Main
# -------------------------

def main() -> int:
    if not CROSSREF_MAILTO or "@" not in CROSSREF_MAILTO:
        return die(
            "Set CROSSREF_MAILTO to a real email address. Crossref recommends polite API access with contact info."
        )

    allowlist = read_doi_list(Path("config/doi_allowlist.txt"))
    blocklist = read_doi_list(Path("config/doi_blocklist.txt"))

    try:
        candidates = collect_candidates()
    except requests.RequestException as exc:
        return die(f"Crossref request failed: {exc}")

    if not candidates:
        return die("No records returned from Crossref.")

    best_by_key: Dict[str, Dict[str, Any]] = {}
    debug_rows: List[Dict[str, Any]] = []

    for item in candidates:
        doi = normalize_doi(item.get("DOI", ""))
        title_key = safe_first(item.get("title")).lower()
        if not doi and not title_key:
            continue

        score, reasons, diagnostics = score_item(item)
        record = build_record(item, score, reasons)
        dedupe_key = doi or f"title:{title_key}"

        debug_rows.append({
            "included": include_item(item, score, diagnostics, allowlist, blocklist),
            "dedupe_key": dedupe_key,
            "title": record["title"],
            "doi": record["doi"],
            "year": record["year"],
            "score": score,
            "reasons": reasons,
            "diagnostics": diagnostics,
        })

        if not include_item(item, score, diagnostics, allowlist, blocklist):
            continue

        existing = best_by_key.get(dedupe_key)
        if existing is None or record["score"] > existing["score"]:
            best_by_key[dedupe_key] = record

    publications = list(best_by_key.values())
    publications.sort(
        key=lambda pub: (
            -(pub["year"] or 0),
            pub.get("published_date") or "",
            -int(pub.get("score") or 0),
            pub["title"].lower(),
        ),
        reverse=False,
    )

    OUTPUT_FILE.write_text(
        json.dumps(publications, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    if os.getenv("WRITE_DEBUG", "0") == "1":
        debug_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target_author": TARGET_AUTHOR,
            "target_orcid": TARGET_ORCID,
            "candidate_count": len(candidates),
            "publication_count": len(publications),
            "rows": debug_rows,
        }
        DEBUG_OUTPUT_FILE.write_text(
            json.dumps(debug_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    print(f"Wrote {len(publications)} publications to {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
