#!/usr/bin/env python3
"""
Build publications.json for the Platt Labs Squarespace gallery.
Sources: MyNCBI public bibliography, ORCID, PubMed E-utilities, Crossref, and manual/Google Scholar seed files.
Google Scholar is not scraped automatically because GitHub Actions routinely hits CAPTCHA/anti-bot pages.
"""
from __future__ import annotations

import html
import json
import os
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
from bs4 import BeautifulSoup

TARGET_AUTHOR = os.getenv("TARGET_AUTHOR", "Michael L. Platt")
TARGET_ORCID = os.getenv("TARGET_ORCID", "0000-0003-3912-8821")
MYNCBI_PUBLIC_URL = os.getenv("MYNCBI_PUBLIC_URL", "https://www.ncbi.nlm.nih.gov/myncbi/plattlab/bibliography/public/")
GOOGLE_SCHOLAR_PROFILE_URL = os.getenv("GOOGLE_SCHOLAR_PROFILE_URL", "https://scholar.google.com/citations?user=U9Hu2rcAAAAJ&hl=en")
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", "publications.json"))
REPORT_FILE = Path(os.getenv("REPORT_FILE", "publication_update_report.json"))
CROSSREF_MAILTO = os.getenv("CROSSREF_MAILTO", "").strip()
NCBI_EMAIL = os.getenv("NCBI_EMAIL", CROSSREF_MAILTO).strip()
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "").strip()
MAX_MYNCBI_PAGES = int(os.getenv("MAX_MYNCBI_PAGES", "20"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
CROSSREF_ROWS = int(os.getenv("CROSSREF_ROWS", "100"))
MAX_CROSSREF_NAME_PAGES = int(os.getenv("MAX_CROSSREF_NAME_PAGES", "2"))
MIN_NAME_SEARCH_SCORE = int(os.getenv("MIN_NAME_SEARCH_SCORE", "120"))
CONFIG_DIR = Path("config")

SOURCE_PRIORITY = {
    "myncbi_pubmed": 100,
    "pubmed": 95,
    "myncbi": 92,
    "orcid": 88,
    "crossref_doi": 84,
    "manual": 82,
    "google_scholar_seed": 80,
    "crossref_name": 55,
}
AUTHOR_VARIANTS = [
    "Michael L. Platt", "Michael Louis Platt", "Michael Platt", "Michael L Platt",
    "M L Platt", "M. L. Platt", "ML Platt", "Platt ML", "Platt M",
]
CONTEXT_WORDS = {
    "amygdala", "attention", "behavior", "behaviour", "brain", "cayo", "cognition",
    "cognitive", "cortex", "cortical", "decision", "dopamine", "electrophysiology",
    "foraging", "macaque", "monkey", "neural", "neuro", "neuron", "neuronal",
    "neuroscience", "primate", "reward", "social", "striatum", "wharton", "marketing",
    "advertising", "brand", "consumer", "economics", "decision-making",
}
LOW_CONTEXT_WORDS = {"geology", "astronomy", "botany", "mechanical engineering", "civil engineering"}

PREPRINT_TYPE_HINTS = {
    "preprint", "biorxiv", "medrxiv", "psyarxiv", "osf", "openrxiv", "posted-content",
    "preprints", "preprints.org", "research square", "ssrn", "arxiv"
}

AUTHOR_NAME_CORRECTIONS = {
    "lamacchi a": "Alessandro P. Lamacchia",
    "parodi f": "Felipe Parodi",
    "matelsky j": "Jordan K. Matelsky",
    "selgado m": "Melanie Segado",
    "segado m": "Melanie Segado",
    "jiang y": "Yaoguang Jiang",
    "regla vargas a": "Alejandra Regla-Vargas",
    "sofi l": "Liala Sofi",
    "kimock c": "Clare Kimock",
    "waller b": "Bridget M. Waller",
    "kording k": "Konrad P. Kording",
    "platt m": "Michael L. Platt",
    "platt ml": "Michael L. Platt",
    "michael l platt": "Michael L. Platt",
    "michael platt": "Michael L. Platt",
    "rennie sm": "Scott M. Rennie",
    "scott m rennie": "Scott M. Rennie",
}

PRIMATEFACE_AUTHORS = [
    "Felipe Parodi",
    "Jordan K. Matelsky",
    "Alessandro P. Lamacchia",
    "Melanie Segado",
    "Yaoguang Jiang",
    "Alejandra Regla-Vargas",
    "Liala Sofi",
    "Clare Kimock",
    "Bridget M. Waller",
    "Michael L. Platt",
    "Konrad P. Kording",
]

@dataclass
class Pub:
    title: str = ""
    authors: List[str] = field(default_factory=list)
    journal: str = ""
    publisher: str = ""
    year: Optional[int] = None
    published_date: str = ""
    doi: str = ""
    pmid: str = ""
    pmcid: str = ""
    url: str = ""
    type: str = ""
    abstract: str = ""
    thumbnail_url: str = ""
    sources: Set[str] = field(default_factory=set)
    source_priority: int = 0
    raw_citation: str = ""
    score: int = 0

    def key(self) -> str:
        if self.doi:
            return "doi:" + self.doi
        if self.pmid:
            return "pmid:" + self.pmid
        return "title:" + norm_title(self.title)

    def merge(self, other: "Pub") -> "Pub":
        self.sources |= other.sources
        self.source_priority = max(self.source_priority, other.source_priority)
        self.score = max(self.score, other.score)
        for attr in ["title", "journal", "publisher", "published_date", "doi", "pmid", "pmcid", "url", "type", "abstract", "thumbnail_url", "raw_citation"]:
            if not getattr(self, attr) and getattr(other, attr):
                setattr(self, attr, getattr(other, attr))
        if other.title and len(other.title) > len(self.title) + 10:
            self.title = other.title
        if other.journal and (not self.journal or len(other.journal) > len(self.journal)):
            self.journal = other.journal
        if other.year and not self.year:
            self.year = other.year
        self.authors = merge_authors(self.authors, other.authors)
        normalize_pub(self)
        return self

    def as_json(self) -> Dict[str, Any]:
        normalize_pub(self)
        url = self.url or (f"https://doi.org/{self.doi}" if self.doi else (f"https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/" if self.pmid else ""))
        return {
            "title": self.title or "Untitled",
            "authors": self.authors,
            "authors_display": ", ".join(self.authors),
            "journal": normalized_journal(self),
            "publisher": self.publisher,
            "year": self.year,
            "published_date": self.published_date,
            "doi": self.doi,
            "pmid": self.pmid,
            "pmcid": self.pmcid,
            "url": url,
            "type": normalized_type(self),
            "thumbnail_url": self.thumbnail_url,
            "sources": sorted(self.sources),
            "score": self.score,
        }

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print("WARNING: " + msg, file=sys.stderr, flush=True)

def clean(value: Any) -> str:
    value = html.unescape(str(value or ""))
    # Strip publisher/XML tags such as <scp>DNA</scp> before normalization.
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-").replace("\u2013", "-").replace("\u2014", "-")
    value = value.replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    value = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", value).strip()

def norm_doi(value: Any) -> str:
    value = clean(value)
    # If someone hands us an entire publisher URL, extract the DOI substring.
    found = re.search(r"10\.\d{4,9}/[^\s\"<>]+", value, flags=re.I)
    if found:
        value = found.group(0)
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value, flags=re.I)
    value = re.sub(r"^doi:\s*", "", value, flags=re.I)
    value = value.strip(" .;)\t\n").lower()

    # bioRxiv/medRxiv URLs often appear as ...669927v2.abstract. Crossref and
    # DOI links want the base DOI, otherwise enrichment silently fails and we get
    # ugly abbreviated author lists. Because apparently metadata needed cosplay.
    m = re.match(
        r"^(10\.(?:1101|64898)/\d{4}\.\d{2}\.\d{2}\.\d+)(?:v\d+)?(?:\.(?:abstract|full|article-info|figures-only))?$",
        value,
        flags=re.I,
    )
    if m:
        return m.group(1).lower()
    return value
def norm_title(value: Any) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", clean(value).lower())
    return re.sub(r"\s+", " ", value).strip()


TITLE_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "by", "for", "from", "in", "into", "is", "of", "on", "or", "the", "to", "with",
    "article", "articles", "study", "studies", "research", "report", "reports", "review",
}

def title_fingerprint(value: Any) -> str:
    """Loose title key used only after DOI/PMID and exact-title merging.

    It catches duplicates where one source keeps punctuation/subtitles and another
    source strips them. It deliberately requires several meaningful words so we do
    not merge unrelated short titles like sensible software gremlins.
    """
    title = norm_title(value)
    if not title:
        return ""
    tokens = [t for t in title.split() if len(t) > 2 and t not in TITLE_STOPWORDS]
    if len(tokens) < 6:
        return ""
    return " ".join(tokens[:14])

def titles_compatible(a: "Pub", b: "Pub") -> bool:
    """Return True when two records are safe to merge by loose title.

    Requirements:
    - same loose fingerprint
    - years are the same or within 1 year, when both are known
    - at least one shared strong identifier OR substantial exact title overlap
    """
    if title_fingerprint(a.title) != title_fingerprint(b.title):
        return False
    if a.year and b.year and abs(a.year - b.year) > 1:
        return False
    if a.doi and b.doi and a.doi != b.doi:
        return False
    if a.pmid and b.pmid and a.pmid != b.pmid:
        return False
    a_words = set(norm_title(a.title).split())
    b_words = set(norm_title(b.title).split())
    if not a_words or not b_words:
        return False
    overlap = len(a_words & b_words) / max(1, min(len(a_words), len(b_words)))
    return overlap >= 0.78

def load_author_name_corrections() -> Dict[str, str]:
    """Load built-in and optional config/author_aliases.json corrections."""
    corrections = dict(AUTHOR_NAME_CORRECTIONS)
    path = CONFIG_DIR / "author_aliases.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for raw, corrected in data.items():
                    corrections[norm_title(raw)] = clean(corrected)
        except Exception as exc:
            warn(f"Could not read {path}: {exc}")
    return corrections

def clean_author(value: Any) -> str:
    value = clean(value).strip(" .;,:")
    value = re.sub(r"\s*,\s*", ", ", value)
    value = re.sub(r"\s+", " ", value)
    key = norm_title(value)
    return load_author_name_corrections().get(key, value)

def author_initials_from_tokens(tokens: Sequence[str]) -> str:
    """Return initials for a list of given/middle-name tokens.

    Full given names contribute their first letter. Short all-caps initial chunks
    such as "LJN" or "JN" contribute the whole chunk. That helps match forms like
    "Lauren JN Brent" with "Brent LJN" without deciding every human named Amy has
    initials AMY, because publication metadata has caused enough harm already.
    """
    initials = []
    for token in tokens:
        raw_pieces = [p for p in re.split(r"[^A-Za-z0-9]+", str(token)) if p]
        for raw_piece in raw_pieces:
            piece = raw_piece.lower()
            if not piece:
                continue
            if len(piece) <= 4 and raw_piece.isupper():
                initials.append(piece)
            else:
                initials.append(piece[0])
    return "".join(initials)


def author_key(value: Any) -> str:
    """Canonical person key for merging 'Rennie SM' with 'Scott M. Rennie'.

    This is intentionally conservative. It keys a person as family-name + initials
    when the string looks like a human name, and falls back to normalized text for
    group authors such as Cayo Biobank Research Unit.
    """
    author = clean_author(value)
    raw = author.lower().replace(".", "")
    raw = raw.replace("\u00a0", " ")
    raw = re.sub(r"\s+", " ", raw).strip(" ,;:")
    if not raw:
        return ""

    # Surname, Given Middle
    if "," in raw:
        family, rest = [part.strip() for part in raw.split(",", 1)]
        family = norm_title(family)
        initials = author_initials_from_tokens(norm_title(rest).split())
        return f"{family}|{initials}" if family and initials else norm_title(raw)

    tokens = norm_title(raw).split()
    if not tokens:
        return ""
    if len(tokens) == 1:
        return tokens[0]

    # Group/consortium authors should not be mangled into fake surname keys.
    group_words = {"unit", "group", "consortium", "collaboration", "committee", "team", "network", "initiative", "project", "biobank", "research"}
    if len(tokens) >= 3 and any(word in tokens for word in group_words):
        return "group|" + " ".join(tokens)

    last = tokens[-1]

    # Surname-first shorthand: Platt ML, Rennie SM, Regla Vargas A, Brent LJN.
    if re.fullmatch(r"[a-z]{1,4}", last) and len(last) <= 4 and not any(len(t) > 1 for t in tokens[1:-1]):
        family = " ".join(tokens[:-1])
        return f"{family}|{last}"

    # Surname-first shorthand with compound surname: Negron Del Valle JE.
    if re.fullmatch(r"[a-z]{1,4}", last) and len(tokens) >= 3:
        likely_initials = last
        likely_family = " ".join(tokens[:-1])
        # Treat as surname-first only when the leading tokens do not look like
        # ordinary given names plus family names. This is not perfect, because names
        # are chaos wearing conference badges, but it catches PubMed abbreviations.
        if all(len(t) > 1 for t in tokens[:-1]):
            return f"{likely_family}|{likely_initials}"

    # Given Middle Family. Preserve hyphenated last names as one family unit
    # before norm_title splits them: Alba Motes-Rodrigo -> motes rodrigo|a.
    raw_words = raw.split()
    if raw_words and "-" in raw_words[-1] and len(tokens) >= 3:
        family_tokens = norm_title(raw_words[-1]).split()
        family = " ".join(family_tokens)
        initials = author_initials_from_tokens(tokens[:-len(family_tokens)])
        return f"{family}|{initials}" if family and initials else norm_title(raw)

    family = tokens[-1]
    initials = author_initials_from_tokens(tokens[:-1])
    return f"{family}|{initials}" if initials else norm_title(raw)

def author_display_quality(value: Any) -> float:
    author = clean_author(value)
    tokens = norm_title(author).split()
    if not author:
        return -100.0

    score = 0.0
    score += min(len(author), 40) / 10.0
    score += 4.0 if len(tokens) >= 2 else 0.0
    score += 3.0 if len(tokens) >= 3 else 0.0
    score += 2.0 if "." in author else 0.0
    score -= 4.0 if re.fullmatch(r"[A-Za-z .-]+", author) and len(tokens) == 2 and len(tokens[-1]) <= 4 and tokens[-1].replace(".", "").isupper() else 0.0
    score -= 2.0 if author.endswith(".") and len(tokens) <= 2 else 0.0
    return score

def is_group_author(value: Any) -> bool:
    n = norm_title(value)
    group_words = {
        "unit", "group", "consortium", "collaboration", "committee", "team",
        "network", "initiative", "project", "biobank", "research", "study",
    }
    return bool(set(n.split()) & group_words)

def author_tokens(value: Any) -> List[str]:
    return norm_title(clean_author(value)).split()

def looks_like_fragment_of(author: str, other: str) -> bool:
    """Detect broken fragments caused by citation parsing/merged sources.

    Examples we drop only when the fuller name exists nearby:
    - "Michael L" when "Michael L. Platt" exists
    - "K" when "K. M. Sharika" exists
    - "Hart J" when "Jordan D. A. Hart" exists
    """
    a_tokens = author_tokens(author)
    b_tokens = author_tokens(other)
    if not a_tokens or not b_tokens or len(b_tokens) <= len(a_tokens):
        return False
    if is_group_author(author) or is_group_author(other):
        return False

    # Prefix fragment: "Michael L" vs "Michael L Platt"; "K" vs "K M Sharika".
    if len(a_tokens) <= 2 and b_tokens[:len(a_tokens)] == a_tokens:
        return True

    # Surname-initial abbreviation fragment: "Hart J" vs "Jordan D A Hart".
    if len(a_tokens) == 2 and len(a_tokens[1]) <= 4:
        family, initials = a_tokens
        if b_tokens[-1] == family:
            full_initials = author_initials_from_tokens(b_tokens[:-1])
            if full_initials.startswith(initials) or initials.startswith(full_initials):
                return True

    return False

def remove_author_fragments(authors: Sequence[str]) -> List[str]:
    cleaned = [clean_author(a) for a in authors if clean_author(a)]
    if len(cleaned) < 2:
        return cleaned

    keep: List[str] = []
    for i, author in enumerate(cleaned):
        if any(i != j and looks_like_fragment_of(author, other) for j, other in enumerate(cleaned)):
            continue
        keep.append(author)
    return keep

def canonical_author_key_matches(a: str, b: str) -> bool:
    ak = author_key(a)
    bk = author_key(b)
    if not ak or not bk:
        return False
    if ak == bk:
        return True
    if "|" not in ak or "|" not in bk:
        return False
    af, ai = ak.split("|", 1)
    bf, bi = bk.split("|", 1)
    if af != bf or not ai or not bi:
        return False
    return ai.startswith(bi) or bi.startswith(ai)

def author_lists_overlap(a: Sequence[str], b: Sequence[str]) -> float:
    if not a or not b:
        return 0.0
    matched = 0
    for left in a:
        if any(canonical_author_key_matches(left, right) for right in b):
            matched += 1
    return matched / max(1, min(len(a), len(b)))

def choose_best_author_list(existing: Sequence[str], incoming: Sequence[str]) -> List[str]:
    """Choose one coherent author list instead of unioning every metadata source.

    Earlier versions unioned MyNCBI abbreviations, PubMed names, and Crossref
    names. That produced displays like "Michael L. Platt, Peter Sterling, Michael L".
    Each source is now treated as a candidate author list; the cleanest complete
    list wins.
    """
    existing_u = remove_author_fragments(uniq(existing))
    incoming_u = remove_author_fragments(uniq(incoming))

    if not existing_u:
        return incoming_u
    if not incoming_u:
        return existing_u

    existing_score = author_list_quality(existing_u)
    incoming_score = author_list_quality(incoming_u)
    overlap = author_lists_overlap(existing_u, incoming_u)

    # Same people, different formatting. Pick the richer display list.
    if overlap >= 0.45:
        return incoming_u if incoming_score > existing_score else existing_u

    # One list is clearly richer. Pick it, do not union it.
    if incoming_score >= existing_score + 4:
        return incoming_u
    if existing_score >= incoming_score + 4:
        return existing_u

    # Last resort: choose the fuller/better list. Unioning is the root problem.
    if len(incoming_u) > len(existing_u):
        return incoming_u
    if len(existing_u) > len(incoming_u):
        return existing_u
    return incoming_u if incoming_score > existing_score else existing_u

def uniq(values: Iterable[str]) -> List[str]:
    order: List[str] = []
    best_by_key: Dict[str, str] = {}

    for value in values:
        cleaned = clean_author(value)
        if not cleaned:
            continue

        key = author_key(cleaned) or cleaned.lower()
        if key not in best_by_key:
            order.append(key)
            best_by_key[key] = cleaned
            continue

        current = best_by_key[key]
        if author_display_quality(cleaned) > author_display_quality(current):
            best_by_key[key] = cleaned

    return remove_author_fragments([best_by_key[key] for key in order if best_by_key.get(key)])


def author_list_quality(authors: Sequence[str]) -> float:
    score = 0.0
    for author in authors:
        cleaned = clean_author(author)
        n = norm_title(cleaned)
        tokens = n.split()
        if len(tokens) >= 2:
            score += 1
        if len(tokens) >= 3:
            score += 2
        if tokens and len(tokens[0]) > 1 and tokens[0] not in {"m", "ml", "a", "f", "j", "k"}:
            score += 2
        if len(cleaned) >= 12:
            score += 1
        if "." in cleaned:
            score += 0.5
    return score + len(uniq(authors)) * 0.1

def merge_authors(existing: Sequence[str], incoming: Sequence[str]) -> List[str]:
    return choose_best_author_list(existing, incoming)


def has_target_author_name(author: str) -> bool:
    """Match Michael L. Platt, but not Jonathan M. Platt or Frances M. Platt."""
    n = norm_title(clean_author(author))
    if not n:
        return False
    patterns = [
        r"^michael\s+platt$",
        r"^michael\s+l\s+platt$",
        r"^michael\s+louis\s+platt$",
        r"^m\s+l\s+platt$",
        r"^ml\s+platt$",
        r"^platt\s+m$",
        r"^platt\s+ml$",
        r"^platt\s+michael$",
        r"^platt\s+michael\s+l$",
    ]
    return any(re.search(p, n) for p in patterns)

def is_preprint(pub: "Pub") -> bool:
    blob = " ".join([pub.type, pub.journal, pub.publisher, pub.url, pub.doi]).lower()

    # bioRxiv/medRxiv legacy DOIs use 10.1101; newer openRxiv DOIs use 10.64898.
    # PsyArXiv/OSF preprints often use 10.31234.
    if pub.doi.startswith(("10.1101/", "10.64898/", "10.31234/")):
        return True

    return any(hint in blob for hint in PREPRINT_TYPE_HINTS)

def normalized_type(pub: "Pub") -> str:
    if is_preprint(pub):
        return "preprint"
    raw = clean(pub.type).lower()
    if raw in {"journal-article", "publication", "article", "journal article"}:
        return "research-article"
    return raw or "research-article"

def normalized_journal(pub: "Pub") -> str:
    journal = clean(pub.journal)
    low = journal.lower()
    blob = " ".join([journal, pub.publisher, pub.url, pub.doi, pub.type]).lower()

    # New openRxiv DOIs use the 10.64898 prefix for bioRxiv/medRxiv. Most Platt
    # Lab openRxiv records here are bioRxiv biology preprints, and Crossref often
    # gives them publisher=openRxiv, type=posted-content, and an empty journal.
    # Without this normalization the Squarespace card falls back to "Research Article",
    # which is the tiny metadata gremlin we are stomping on here.
    if "medrxiv" in blob:
        return "medRxiv"
    if "biorxiv" in blob or pub.doi.startswith(("10.1101/", "10.64898/")):
        return "bioRxiv"
    if "psyarxiv" in blob or pub.doi.startswith("10.31234/"):
        return "PsyArXiv"
    if "ssrn" in blob:
        return "SSRN"
    if journal in {"[preprint]", "preprint", "posted-content"} or "openrxiv" in blob:
        return "Preprint"
    return journal

def normalize_pub(pub: "Pub") -> "Pub":
    pub.title = clean(pub.title).rstrip(" .")
    pub.journal = clean(pub.journal)
    pub.publisher = clean(pub.publisher)
    pub.doi = norm_doi(pub.doi)
    pub.authors = uniq(pub.authors)
    if "primateface" in norm_title(pub.title):
        pub.authors = PRIMATEFACE_AUTHORS
        pub.type = "preprint"
        if not pub.journal or "preprint" in pub.journal.lower():
            pub.journal = "bioRxiv"
        if pub.doi.startswith("10.1101/"):
            pub.url = f"https://www.biorxiv.org/content/{pub.doi}v2"
    if is_preprint(pub):
        pub.type = "preprint"
        pub.journal = normalized_journal(pub)
    return pub

def to_int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None

def first(value: Any) -> str:
    if isinstance(value, list) and value:
        return str(value[0])
    if isinstance(value, str):
        return value
    return ""

def month_num(value: Any) -> Optional[int]:
    value = clean(value).lower()
    table = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    if value.isdigit():
        n = int(value)
        return n if 1 <= n <= 12 else None
    return table.get(value[:3])

def date_string(year: Optional[int], month: Any = "", day: Any = "") -> str:
    if not year:
        return ""
    m = month_num(month) or 1
    d = to_int(day) or 1
    return f"{year:04d}-{m:02d}-{d:02d}"

def text_xml(elem: Optional[ET.Element]) -> str:
    return clean("".join(elem.itertext())) if elem is not None else ""

def extract_year(text: str) -> Optional[int]:
    matches = re.findall(r"\b(19[7-9]\d|20[0-3]\d)\b", text)
    return int(matches[0]) if matches else None

def extract_doi(text: str) -> str:
    m = re.search(r"\b10\.\d{4,9}/[^\s\"<>]+", text, flags=re.I)
    return norm_doi(m.group(0)) if m else ""

def extract_pmid(text: str) -> str:
    m = re.search(r"PubMed PMID:\s*(\d+)", text, flags=re.I)
    return m.group(1) if m else ""

def extract_pmcid(text: str) -> str:
    m = re.search(r"PubMed Central PMCID:\s*(PMC\d+)", text, flags=re.I)
    return m.group(1) if m else ""

def score_text(text: str) -> int:
    low = clean(text).lower()
    score = 20 if "platt" in low else 0
    score += sum(8 for w in CONTEXT_WORDS if w in low)
    score -= sum(12 for w in LOW_CONTEXT_WORDS if w in low)
    return score

def has_platt_author(authors: Sequence[str]) -> bool:
    return any(has_target_author_name(author) for author in authors)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": f"platt-labs-publications-bot/2.0 (mailto:{CROSSREF_MAILTO or NCBI_EMAIL or 'unset'})"})
    return s

def read_doi_file(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    vals = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            vals.add(norm_doi(line))
    return vals

def read_id_file(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    vals = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            vals.add(clean(line))
    return vals

def read_manual(path: Path) -> List[Pub]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    pubs = []
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        authors = item.get("authors", [])
        if isinstance(authors, str):
            authors = [a.strip() for a in authors.split(",")]
        pubs.append(Pub(
            title=clean(item.get("title")), authors=uniq(authors), journal=clean(item.get("journal")),
            publisher=clean(item.get("publisher")), year=to_int(item.get("year")), published_date=clean(item.get("published_date")),
            doi=norm_doi(item.get("doi")), pmid=clean(item.get("pmid")), pmcid=clean(item.get("pmcid")),
            url=clean(item.get("url")), type=clean(item.get("type")) or "manual", thumbnail_url=clean(item.get("thumbnail_url")),
            sources={"manual"}, source_priority=SOURCE_PRIORITY["manual"], score=SOURCE_PRIORITY["manual"]
        ))
    return pubs

def fetch_myncbi(s: requests.Session) -> List[Pub]:
    log("Fetching MyNCBI public bibliography...")
    pubs = []
    for page in range(1, MAX_MYNCBI_PAGES + 1):
        url = MYNCBI_PUBLIC_URL if page == 1 else f"{MYNCBI_PUBLIC_URL}?page={page}"
        r = s.get(url, timeout=REQUEST_TIMEOUT, headers={"Accept": "text/html,*/*"})
        r.raise_for_status()
        got = parse_myncbi(r.text)
        if not got:
            break
        pubs.extend(got)
        log(f"  MyNCBI page {page}: {len(got)} candidates")
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        m = re.search(r"Enter page number of\s+(\d+)", text)
        if m and page >= int(m.group(1)):
            break
        time.sleep(0.25)
    return pubs

def parse_myncbi(markup: str) -> List[Pub]:
    soup = BeautifulSoup(markup, "html.parser")
    text = soup.get_text("\n", strip=True)
    chunks = re.split(r"\n\s*select\s*\n", text, flags=re.I)
    pubs = []
    for chunk in chunks:
        chunk = clean(chunk)
        if not looks_citation(chunk):
            continue
        pub = parse_myncbi_chunk(chunk)
        if pub:
            pubs.append(pub)
    for a in soup.find_all("a", href=True):
        href, title = str(a["href"]), clean(a.get_text(" ", strip=True))
        m = re.search(r"/pubmed/(\d+)|pubmed\.ncbi\.nlm\.nih\.gov/(\d+)", href)
        if m and title:
            pmid = m.group(1) or m.group(2)
            pubs.append(Pub(title=title, pmid=pmid, url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", sources={"myncbi"}, source_priority=SOURCE_PRIORITY["myncbi"], score=SOURCE_PRIORITY["myncbi"]))
    return pubs

def looks_citation(text: str) -> bool:
    low = text.lower()
    return ("platt" in low or "pubmed pmid" in low) and any(x in low for x in ["doi:", "pubmed pmid", "available from", "preprint", "journal", "nature", "science", "biorxiv", "ssrn"])

def parse_myncbi_chunk(text: str) -> Optional[Pub]:
    doi, pmid, pmcid = extract_doi(text), extract_pmid(text), extract_pmcid(text)
    m = re.search(r"Available from:\s*(https?://\S+)", text, flags=re.I)
    url = m.group(1).rstrip(" .;)") if m else ""
    year = extract_year(text)
    published_date = date_string(year)
    mm = re.search(rf"\b{year}\s+([A-Za-z]{{3,9}})\b", text) if year else None
    if mm:
        published_date = date_string(year, mm.group(1))
    authors, title, journal = [], "", ""
    parts = re.split(r"\.\s{2,}", text, maxsplit=1)
    if len(parts) == 2:
        authors = parse_authors(parts[0])
        title = parts[1].split(". ", 1)[0].strip().rstrip(".")
    else:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        if len(sentences) >= 2:
            authors = parse_authors(sentences[0])
            title = sentences[1].strip().rstrip(".")
    jm = re.search(r"\.\s*([^.;]{2,120}?)\.\s*(?:\d{4}|doi:|DOI:|PubMed PMID:)", text)
    if jm and "Platt" not in jm.group(1):
        journal = clean(jm.group(1))
    if not title and doi:
        title = "Publication " + doi
    if not title:
        return None
    if not doi and url:
        doi = extract_doi(url)
    pub_type = "preprint" if any(x in text.lower() for x in ["[preprint]", "biorxiv", "medrxiv", "ssrn"]) or doi.startswith("10.1101/") else "publication"
    pub = Pub(title=clean(title), authors=authors, journal=journal, year=year, published_date=published_date, doi=doi, pmid=pmid, pmcid=pmcid, url=url or (f"https://doi.org/{doi}" if doi else (f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else "")), type=pub_type, sources={"myncbi"}, source_priority=SOURCE_PRIORITY["myncbi"], raw_citation=text, score=SOURCE_PRIORITY["myncbi"] + score_text(text))
    return normalize_pub(pub)

def parse_authors(text: str) -> List[str]:
    text = re.sub(r"\bet al\.?", "", text, flags=re.I)
    return uniq([p.strip() for p in text.split(",") if 1 < len(p.strip()) < 90])

def nested(obj: Any, keys: Sequence[str], default: Any = "") -> Any:
    cur = obj
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur

def fetch_orcid(s: requests.Session) -> List[Pub]:
    log("Fetching ORCID works...")
    url = f"https://pub.orcid.org/v3.0/{TARGET_ORCID}/works"
    r = s.get(url, headers={"Accept": "application/json"}, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        warn(f"ORCID returned HTTP {r.status_code}; skipping")
        return []
    pubs = []
    for group in r.json().get("group", []) or []:
        for item in group.get("work-summary", []) or []:
            title = clean(nested(item, ["title", "title", "value"]))
            journal = clean(nested(item, ["journal-title", "value"]))
            doi = pmid = pmcid = ""
            ext = nested(item, ["external-ids", "external-id"], [])
            ext = [ext] if isinstance(ext, dict) else (ext or [])
            for e in ext:
                typ, val = clean(e.get("external-id-type", "")).lower(), clean(e.get("external-id-value", ""))
                if typ == "doi":
                    doi = norm_doi(val)
                elif typ == "pmid":
                    pmid = val
                elif typ in {"pmc", "pmcid"}:
                    pmcid = val if val.upper().startswith("PMC") else "PMC" + val
            year = to_int(nested(item, ["publication-date", "year", "value"]))
            date = date_string(year, nested(item, ["publication-date", "month", "value"]), nested(item, ["publication-date", "day", "value"]))
            url = clean(nested(item, ["url", "value"])) or (f"https://doi.org/{doi}" if doi else (f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""))
            if title or doi or pmid:
                pubs.append(Pub(title=title, journal=journal, year=year, published_date=date, doi=doi, pmid=pmid, pmcid=pmcid, url=url, type=clean(item.get("type")), sources={"orcid"}, source_priority=SOURCE_PRIORITY["orcid"], score=SOURCE_PRIORITY["orcid"]))
    log(f"  ORCID: {len(pubs)} candidates")
    return pubs

def batches(values: Sequence[str], n: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(values), n):
        yield values[i:i+n]

def fetch_pubmed(s: requests.Session, pmids: Sequence[str], myncbi_pmids: Set[str]) -> List[Pub]:
    pmids = sorted({p for p in pmids if str(p).isdigit()})
    if not pmids:
        return []
    log(f"Fetching PubMed XML for {len(pmids)} PMIDs...")
    pubs = []
    for batch in batches(pmids, 150):
        params = {"db":"pubmed", "id": ",".join(batch), "retmode":"xml", "tool":"platt-labs-publications-bot"}
        if NCBI_EMAIL:
            params["email"] = NCBI_EMAIL
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY
        r = s.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi", params=params, timeout=REQUEST_TIMEOUT, headers={"Accept":"application/xml"})
        r.raise_for_status()
        for pub in parse_pubmed(r.text):
            if pub.pmid in myncbi_pmids:
                pub.sources.add("myncbi_pubmed")
                pub.source_priority = max(pub.source_priority, SOURCE_PRIORITY["myncbi_pubmed"])
            pubs.append(pub)
        time.sleep(0.35 if not NCBI_API_KEY else 0.12)
    log(f"  PubMed: {len(pubs)} candidates")
    return pubs

def parse_pubmed(xml_text: str) -> List[Pub]:
    root = ET.fromstring(xml_text)
    pubs = []
    for pma in root.findall(".//PubmedArticle"):
        article = pma.find(".//Article")
        med = pma.find("MedlineCitation")
        if article is None or med is None:
            continue
        pmid = text_xml(med.find("PMID"))
        title = text_xml(article.find("ArticleTitle"))
        journal = text_xml(article.find("./Journal/Title")) or text_xml(article.find("./Journal/ISOAbbreviation"))
        year, date = parse_pubmed_date(article)
        authors = []
        for au in article.findall(".//AuthorList/Author"):
            coll = text_xml(au.find("CollectiveName"))
            last, fore, initials = text_xml(au.find("LastName")), text_xml(au.find("ForeName")), text_xml(au.find("Initials"))
            if coll:
                authors.append(coll)
            elif last and fore:
                authors.append(f"{fore} {last}")
            elif last and initials:
                authors.append(f"{initials} {last}")
        doi = pmcid = ""
        for aid in pma.findall(".//PubmedData/ArticleIdList/ArticleId"):
            typ, val = aid.attrib.get("IdType", "").lower(), text_xml(aid)
            if typ == "doi":
                doi = norm_doi(val)
            elif typ in {"pmc", "pmcid"}:
                pmcid = val
        abstract = clean(" ".join(text_xml(x) for x in article.findall(".//Abstract/AbstractText")))
        url = f"https://doi.org/{doi}" if doi else f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        pubs.append(Pub(title=title, authors=uniq(authors), journal=journal, year=year, published_date=date, doi=doi, pmid=pmid, pmcid=pmcid, url=url, type="journal-article", abstract=abstract, sources={"pubmed"}, source_priority=SOURCE_PRIORITY["pubmed"], score=SOURCE_PRIORITY["pubmed"] + score_text(" ".join([title, journal, abstract]))))
    return pubs

def parse_pubmed_date(article: ET.Element) -> Tuple[Optional[int], str]:
    ad = article.find("./ArticleDate")
    if ad is not None:
        y = to_int(text_xml(ad.find("Year")))
        return y, date_string(y, text_xml(ad.find("Month")), text_xml(ad.find("Day")))
    pd = article.find("./Journal/JournalIssue/PubDate")
    if pd is not None:
        y = to_int(text_xml(pd.find("Year"))) or extract_year(text_xml(pd.find("MedlineDate")))
        return y, date_string(y, text_xml(pd.find("Month")), text_xml(pd.find("Day")))
    return None, ""

def fetch_crossref_dois(s: requests.Session, dois: Sequence[str]) -> List[Pub]:
    dois = sorted({norm_doi(d) for d in dois if norm_doi(d)})
    if not dois:
        return []
    log(f"Fetching Crossref metadata for {len(dois)} DOIs...")
    pubs = []
    for doi in dois:
        try:
            params = {"mailto": CROSSREF_MAILTO} if CROSSREF_MAILTO else {}
            r = s.get(f"https://api.crossref.org/works/{doi}", params=params, headers={"Accept":"application/json"}, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            pub = parse_crossref(r.json().get("message", {}), "crossref_doi")
            if pub:
                pubs.append(pub)
        except requests.RequestException as e:
            warn(f"Crossref DOI lookup failed for {doi}: {e}")
        time.sleep(0.08)
    log(f"  Crossref DOI: {len(pubs)} candidates")
    return pubs

def fetch_crossref_names(s: requests.Session) -> List[Pub]:
    log("Fetching Crossref name-search fallback...")
    pubs = []
    for variant in AUTHOR_VARIANTS:
        cursor = "*"
        for _ in range(MAX_CROSSREF_NAME_PAGES):
            params = {"query.author": variant, "rows": CROSSREF_ROWS, "cursor": cursor, "sort":"published", "order":"desc"}
            if CROSSREF_MAILTO:
                params["mailto"] = CROSSREF_MAILTO
            try:
                r = s.get("https://api.crossref.org/works", params=params, headers={"Accept":"application/json"}, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
            except requests.RequestException as e:
                warn(f"Crossref name search failed for {variant}: {e}")
                break
            msg = r.json().get("message", {})
            for item in msg.get("items", []) or []:
                pub = parse_crossref(item, "crossref_name")
                if pub and has_platt_author(pub.authors) and pub.score >= MIN_NAME_SEARCH_SCORE:
                    pubs.append(pub)
            nxt = msg.get("next-cursor")
            if not nxt or nxt == cursor:
                break
            cursor = nxt
            time.sleep(0.15)
    log(f"  Crossref name fallback: {len(pubs)} candidates kept")
    return pubs

def parse_crossref(item: Dict[str, Any], source: str) -> Optional[Pub]:
    doi, title = norm_doi(item.get("DOI", "")), clean(first(item.get("title")))
    if not title and not doi:
        return None
    authors = []
    for a in item.get("author", []) or []:
        given, family, literal = clean(a.get("given", "")), clean(a.get("family", "")), clean(a.get("literal", ""))
        full = clean(f"{given} {family}") if given or family else literal
        if full:
            authors.append(full)
    year, date = crossref_date(item)
    journal = clean(first(item.get("container-title")))
    publisher = clean(item.get("publisher", ""))
    abstract = clean(re.sub(r"<[^>]+>", " ", str(item.get("abstract", ""))))
    score = SOURCE_PRIORITY[source] + score_text(" ".join([title, journal, publisher, abstract, " ".join(authors)]))
    if has_platt_author(authors):
        score += 80
    if any(TARGET_ORCID in str(a.get("ORCID", "")) for a in item.get("author", []) or []):
        score += 150
    return Pub(title=title, authors=uniq(authors), journal=journal, publisher=publisher, year=year, published_date=date, doi=doi, url=f"https://doi.org/{doi}" if doi else clean(item.get("URL", "")), type=clean(item.get("type", "")), abstract=abstract, sources={source}, source_priority=SOURCE_PRIORITY[source], score=score)

def crossref_date(item: Dict[str, Any]) -> Tuple[Optional[int], str]:
    for k in ["published-print", "published-online", "published", "issued", "created"]:
        parts = item.get(k, {}).get("date-parts")
        if isinstance(parts, list) and parts and parts[0]:
            p = parts[0]
            y = to_int(p[0] if len(p) > 0 else None)
            return y, date_string(y, p[1] if len(p) > 1 else "", p[2] if len(p) > 2 else "")
    return None, ""

def google_scholar_seed() -> List[Pub]:
    dois = read_doi_file(CONFIG_DIR / "google_scholar_dois.txt")
    pubs = [Pub(doi=d, url=f"https://doi.org/{d}", sources={"google_scholar_seed"}, source_priority=SOURCE_PRIORITY["google_scholar_seed"], score=SOURCE_PRIORITY["google_scholar_seed"]) for d in dois]
    if pubs:
        log(f"Loaded {len(pubs)} Google Scholar DOI seed candidates")
    return pubs

def merge_all(pubs: Sequence[Pub]) -> Dict[str, Pub]:
    merged: Dict[str, Pub] = {}
    pubs = [normalize_pub(pub) for pub in pubs]

    # Pass 1: merge by canonical identifiers.
    # DOI wins first, then PMID, then exact normalized title.
    for pub in pubs:
        if not (pub.title or pub.doi or pub.pmid):
            continue
        key = pub.key()
        merged[key] = merged[key].merge(pub) if key in merged else pub

    # Pass 2: merge exact title duplicates that came in under different IDs.
    by_title: Dict[str, str] = {}
    for key, pub in list(merged.items()):
        tk = norm_title(pub.title)
        if len(tk) < 20:
            continue
        if tk in by_title and by_title[tk] in merged:
            merged[by_title[tk]].merge(pub)
            del merged[key]
        else:
            by_title[tk] = key

    # Pass 3: conservative loose-title merge for source formatting differences.
    # This catches cases like punctuation/subtitle/capitalization variations without
    # merging unrelated short titles, because publication metadata already enjoys
    # being chaotic enough.
    by_fingerprint: Dict[str, str] = {}
    for key, pub in list(merged.items()):
        fp = title_fingerprint(pub.title)
        if not fp:
            continue
        existing_key = by_fingerprint.get(fp)
        if existing_key and existing_key in merged and titles_compatible(merged[existing_key], pub):
            merged[existing_key].merge(pub)
            del merged[key]
        else:
            by_fingerprint[fp] = key

    return merged

def has_target_evidence(pub: Pub) -> bool:
    if has_platt_author(pub.authors):
        return True
    if "orcid" in pub.sources:
        # ORCID works come from Michael L. Platt's own ORCID record. Some ORCID
        # summaries do not include a full author list, so this remains trusted.
        return True
    if has_target_author_name(pub.raw_citation):
        return True
    return False

def filter_pubs(merged: Dict[str, Pub]) -> List[Pub]:
    allow = read_doi_file(CONFIG_DIR / "doi_allowlist.txt")
    doi_block = read_doi_file(CONFIG_DIR / "doi_blocklist.txt")
    pmid_block = read_id_file(CONFIG_DIR / "pmid_blocklist.txt")
    user_curated = {"manual", "google_scholar_seed"}
    out = []
    for pub in merged.values():
        normalize_pub(pub)
        if pub.doi and pub.doi in doi_block:
            continue
        if pub.pmid and pub.pmid in pmid_block:
            continue
        if pub.doi and pub.doi in allow:
            out.append(pub); continue
        if pub.sources & user_curated:
            out.append(pub); continue

        # Critical quality gate: MyNCBI is a lab bibliography, not a guaranteed
        # target-author list. Crossref name search also catches Jonathan/Frances
        # Platt. Keep a record only when Michael L. Platt is actually an author
        # or it came directly from the target ORCID record.
        if not has_target_evidence(pub):
            continue

        if "crossref_name" in pub.sources and not ({"myncbi", "myncbi_pubmed", "pubmed", "orcid", "crossref_doi"} & pub.sources):
            if score_text(" ".join([pub.title, pub.journal, pub.abstract, " ".join(pub.authors)])) < 25:
                continue
        out.append(pub)
    return out

def source_counts(pubs: Sequence[Pub]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for pub in pubs:
        for src in pub.sources:
            counts[src] = counts.get(src, 0) + 1
    return dict(sorted(counts.items()))

def main() -> int:
    if not CROSSREF_MAILTO:
        warn("CROSSREF_MAILTO is not set. Add it as a GitHub Actions secret.")
    s = make_session()
    all_pubs: List[Pub] = []
    all_pubs.extend(read_manual(CONFIG_DIR / "manual_publications.json"))
    all_pubs.extend(google_scholar_seed())
    myncbi = fetch_myncbi(s)
    all_pubs.extend(myncbi)
    all_pubs.extend(fetch_orcid(s))
    myncbi_pmids = {p.pmid for p in myncbi if p.pmid}
    all_pubs.extend(fetch_pubmed(s, [p.pmid for p in all_pubs if p.pmid], myncbi_pmids))
    all_pubs.extend(fetch_crossref_dois(s, [p.doi for p in all_pubs if p.doi]))
    all_pubs.extend(fetch_crossref_names(s))
    all_pubs = [normalize_pub(pub) for pub in all_pubs]
    merged = merge_all(all_pubs)
    pubs = filter_pubs(merged)
    pubs.sort(key=lambda p: (-(p.year or 0), p.published_date or "", -p.source_priority, p.title.lower()))
    out = [p.as_json() for p in pubs]
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_author": TARGET_AUTHOR,
        "target_orcid": TARGET_ORCID,
        "source_urls": {"orcid": f"https://orcid.org/{TARGET_ORCID}", "myncbi": MYNCBI_PUBLIC_URL, "google_scholar": GOOGLE_SCHOLAR_PROFILE_URL},
        "counts": {"candidates": len(all_pubs), "merged": len(merged), "published": len(out)},
        "source_counts": source_counts(pubs),
        "author_strategy": "single_best_source_list_with_fragment_pruning",
    }
    REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log(f"Wrote {len(out)} publications to {OUTPUT_FILE}")
    log(f"Wrote report to {REPORT_FILE}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
