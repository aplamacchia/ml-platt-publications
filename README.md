# Platt Labs publication gallery

This repository generates `publications.json` for the Squarespace publication gallery.

## Sources

The updater combines:

1. MyNCBI public bibliography
   `https://www.ncbi.nlm.nih.gov/myncbi/plattlab/bibliography/public/`

2. ORCID
   `https://orcid.org/0000-0003-3912-8821`

3. PubMed E-utilities enrichment for PMIDs found in MyNCBI

4. Crossref DOI enrichment for DOIs found in MyNCBI, ORCID, PubMed, and manual seed files

5. Optional Google Scholar/manual reconciliation files:
   - `config/google_scholar_dois.txt`
   - `config/manual_publications.json`

Google Scholar is not scraped automatically because it has no stable official public API for this use case and automated requests often hit CAPTCHA/anti-bot responses. Use the seed files when Scholar reveals a missing item.

## GitHub secret

The workflow expects this repository secret:

```text
CROSSREF_MAILTO
```

Use a real email address. It is used for Crossref polite API contact and as the NCBI email value.

## Running the updater manually

In GitHub:

```text
Actions -> Update publications -> Run workflow
```

After it completes, check:

```text
publications.json
publication_update_report.json
```

## Adding missing items from Google Scholar

If Google Scholar shows a paper missing from the site:

1. Find the DOI.
2. Add it to `config/google_scholar_dois.txt`.
3. Commit the change.
4. Run the workflow.

If the item has no DOI, add it manually to `config/manual_publications.json`.

Example:

```json
[
  {
    "title": "Example paper title",
    "authors": ["Michael L. Platt", "Jane Doe"],
    "journal": "Example Journal",
    "year": 2025,
    "url": "https://example.com/article",
    "thumbnail_url": ""
  }
]
```

## Squarespace

Squarespace should not need changes as long as it fetches:

```text
https://aplamacchia.github.io/ml-platt-publications/publications.json
```

This updater preserves the same JSON shape used by the current front-end.


## v3 quality fixes

This update adds stricter Michael L. Platt author gating, normalizes bioRxiv/preprint records, strips publisher XML tags from titles before deduplication, supports PMID blocklisting, and corrects known abbreviated author display issues such as PrimateFace.

New optional config files:

```text
config/pmid_blocklist.txt
config/author_aliases.json
```


## v4 note: bioRxiv/openRxiv normalization

Records with legacy bioRxiv DOIs (`10.1101/...`) and newer openRxiv DOIs (`10.64898/...`) are normalized as preprints. For Platt Lab records where Crossref returns `publisher=openRxiv`, `type=posted-content`, and an empty journal field, the generated JSON now uses `journal: "bioRxiv"` and `type: "preprint"` so the Squarespace card does not fall back to a generic "Research Article" label.


## Author list strategy

The updater deliberately keeps a single best author list per publication rather than unioning names from every metadata source. PubMed, MyNCBI, Crossref, and ORCID often provide the same authors in different forms, such as `Platt ML`, `Michael L Platt`, and `Michael L. Platt`. Unioning those lists creates duplicate or broken displays. The script now scores each source's author list, removes obvious fragments like `Michael L` when `Michael L. Platt` is present, and keeps the cleanest complete list.
