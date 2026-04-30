# Michael L. Platt Publications Gallery

This repo powers an auto-updating publications gallery for a Squarespace site.

## How it works

1. `scripts/update_publications.py` fetches publication metadata from Crossref.
2. GitHub Actions runs the script daily and writes `publications.json`.
3. GitHub Pages hosts `publications.json`.
4. A Squarespace Code Block fetches that JSON and renders publication cards.

## Files

```text
.github/workflows/update-publications.yml
scripts/update_publications.py
squarespace/embed.html
config/doi_allowlist.txt
config/doi_blocklist.txt
requirements.txt
publications.json
```

## First setup

### 1. Create the GitHub repository

Create a public GitHub repository, then add these files.

### 2. Add a repository secret

In GitHub:

Settings → Secrets and variables → Actions → New repository secret

Name:

```text
CROSSREF_MAILTO
```

Value:

```text
your-real-email@example.com
```

Crossref recommends identifying yourself for polite API access.

### 3. Enable GitHub Pages

In GitHub:

Settings → Pages → Build and deployment → Source: Deploy from a branch

Use:

```text
Branch: main
Folder: /root
```

Your JSON URL will be:

```text
https://YOUR_GITHUB_USERNAME.github.io/YOUR_REPO_NAME/publications.json
```

### 4. Run the workflow once

Actions → Update publications → Run workflow

After it finishes, open the JSON URL above in your browser.

### 5. Add the Squarespace block

Open `squarespace/embed.html`.

Replace:

```text
YOUR_GITHUB_USERNAME
YOUR_REPO_NAME
```

Paste the whole file into a Squarespace Code Block on the publications page.

## Managing false positives

If a wrong paper appears, add its DOI to:

```text
config/doi_blocklist.txt
```

If a correct paper is missing but Crossref returns it with weak metadata, add its DOI to:

```text
config/doi_allowlist.txt
```

Commit the change and rerun the workflow.

## Local test

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export CROSSREF_MAILTO="your-real-email@example.com"
python scripts/update_publications.py
python -m json.tool publications.json
```
