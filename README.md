# Search-and-Rescue

Scrape YouTube video descriptions, pull every link, and find domains that are no longer
registered — prime candidates for "rescue" (i.e. re-registration). Results are written
to `report.csv`; the bundled `report.html` is a static viewer that fetches and renders
that CSV client-side.

## Install

```bash
pip install -r requirements.txt
```

Python 3.10+.

## Usage

```bash
# 1. Put one search query per line
printf 'python tutorial\nrust tutorial\n' > queries.txt

# 2. Run
python search_and_rescue.py \
    --queries-file queries.txt \
    --min-views 1000 \
    --max-results-per-query 50 \
    --workers 10

# 3. View the report (any static server works)
python -m http.server 8000
# then open http://localhost:8000/report.html
```

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queries-file` | *(required)* | Text file, one query per line. Blank lines and `#` comments ignored. |
| `--min-views` | `1000` | Skip videos under this view count. |
| `--max-results-per-query` | `50` | How many videos to pull per query. |
| `--workers` | `10` | Thread pool size (YouTube + RDAP calls run in parallel). |
| `--cache-dir` | `./cache` | Where the JSON caches live. |
| `--output` | `./report.csv` | CSV output path. |
| `--flush-every` | `100` | Rewrite the CSV every *N* newly-found available domains. |
| `-v`, `--verbose` | off | Debug logging. |

## How it works

1. Queries are loaded and normalized. Queries already present in
   `cache/queries.json` with an equal-or-larger `--max-results-per-query` are reused
   verbatim — no YouTube traffic.
2. For each unseen video, the description is pulled, URLs are regex-extracted, and
   each URL is reduced to its registrable domain (`tldextract`). The result is
   cached in `cache/videos.json` keyed by video id.
3. Each unique domain is checked against `https://rdap.org/domain/<domain>`. A 404
   means "available" (the domain is unregistered); 200 means "registered"; anything
   else is "unknown". Results land in `cache/domains.json`.
4. For available domains, a static TLD price table is consulted to estimate the
   purchase price and build a registrar search URL.
5. Rows accumulate in `cache/rows.json` and are flushed to `report.csv` every
   `--flush-every` new available domains (default 100) and once more at shutdown.
   The CSV is always sorted by video views descending, then domain ascending.

## Idempotency

Everything expensive is cached. A repeat run with the same arguments does zero
network work and produces a byte-identical `report.csv`. You can delete any single
cache file to force that layer to refresh — for example, wipe `cache/domains.json`
to re-check availability but keep the video descriptions.

## Viewer

`report.html` is a single static file with no build step and no external
dependencies. It fetches `report.csv`, parses it in the browser, and renders a
sortable/filterable table with:

- click-to-sort column headers
- a substring filter box
- an "Only available" toggle
- direct links to both the YouTube video and the registrar's search page

Because `fetch()` over `file://` is blocked in most browsers, serve the folder
with any static HTTP server, e.g. `python -m http.server`.

## Limitations

- `youtube-search-python` uses YouTube's internal search endpoint; very long runs
  may be rate-limited. Use `--workers 4` and smaller `--max-results-per-query` if
  you hit errors.
- RDAP is accurate for gTLDs and most ccTLDs but not all; unknown responses are
  reported as `unknown` rather than guessed.
- Purchase prices are estimates from a built-in TLD table, not live registrar
  quotes.
