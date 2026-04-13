#!/usr/bin/env python3
"""Search YouTube, extract links from video descriptions, report expired domains.

Pipeline per run:
    1. Read queries from --queries-file.
    2. For each query, search YouTube via youtube-search-python (cached per query).
    3. For each video (cached per video id), extract domains from the description.
    4. For each unique registrable domain (cached per domain), ask rdap.org whether
       it's registered; if not, estimate purchase price from a built-in TLD table.
    5. Accumulate rows (video_url, domain, views, status, price, registrar, buy_url)
       and flush them to a CSV every 100 newly-found available domains (also at
       shutdown). `report.html` is a static viewer that fetches + renders the CSV.

Everything expensive is cached under ./cache/ so reruns are idempotent.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
import tldextract
from youtubesearchpython import VideosSearch


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = "search-and-rescue/1.0 (+https://github.com/rohittp0/search-and-rescue)"
RDAP_URL = "https://rdap.org/domain/{}"
RDAP_TIMEOUT = 10
RDAP_RETRY_SLEEP = 2.0
FLUSH_EVERY_N_AVAILABLE = 100

CSV_COLUMNS = [
    "video_id",
    "video_url",
    "video_title",
    "views",
    "domain",
    "status",
    "price",
    "registrar",
    "buy_url",
    "checked_at",
]

# Domains that will never be "expired" and would just create noise.
IGNORED_DOMAINS = frozenset(
    {
        "youtube.com",
        "youtu.be",
        "google.com",
        "googleusercontent.com",
        "gstatic.com",
        "ggpht.com",
        "t.co",
        "bit.ly",
        "goo.gl",
        "lnk.to",
        "instagram.com",
        "twitter.com",
        "x.com",
        "facebook.com",
        "fb.com",
        "tiktok.com",
        "patreon.com",
        "discord.gg",
        "discord.com",
        "amazon.com",
        "amzn.to",
        "spotify.com",
        "apple.com",
        "linkedin.com",
        "github.com",
        "gitlab.com",
        "paypal.com",
        "reddit.com",
        "medium.com",
        "twitch.tv",
    }
)

# (price_estimate, registrar_name, buy_url_template_with_one_{}_placeholder)
TLD_PRICES: dict[str, tuple[str, str, str]] = {
    "com": ("$10.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "net": ("$12.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "org": ("$12.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "io": ("$39.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "co": ("$25.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "dev": ("$14.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "app": ("$15.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "xyz": ("$1.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "ai": ("$89.00", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "me": ("$19.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "info": ("$3.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "biz": ("$5.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "tv": ("$29.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "cc": ("$11.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "store": ("$4.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "online": ("$3.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "site": ("$3.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
    "tech": ("$5.99", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}"),
}
DEFAULT_PRICE = ("~$15 (est.)", "Namecheap", "https://www.namecheap.com/domains/registration/results/?domain={}")


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(data, fp, indent=2, sort_keys=True, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fp:
            return json.load(fp)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Could not read %s (%s); starting fresh.", path, exc)
        return default


def normalize_query(q: str) -> str:
    return " ".join(q.lower().split())


def _row_key(video_id: str, domain: str) -> str:
    return f"{video_id}|{domain}"


class Cache:
    """Four JSON-backed stores, guarded by a single RLock for simplicity."""

    def __init__(self, cache_dir: Path) -> None:
        self.dir = cache_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.queries_path = self.dir / "queries.json"
        self.videos_path = self.dir / "videos.json"
        self.domains_path = self.dir / "domains.json"
        self.rows_path = self.dir / "rows.json"

        self.lock = threading.RLock()
        self.queries: dict[str, dict] = _load_json(self.queries_path, {})
        self.videos: dict[str, dict] = _load_json(self.videos_path, {})
        self.domains: dict[str, dict] = _load_json(self.domains_path, {})
        self.rows: dict[str, dict] = _load_json(self.rows_path, {})

        self._dirty = False

    # -- queries ------------------------------------------------------------
    def has_query(self, query: str, max_results: int) -> bool:
        with self.lock:
            entry = self.queries.get(normalize_query(query))
            return bool(entry) and entry.get("max_results", 0) >= max_results

    def get_cached_video_ids(self, query: str) -> list[str]:
        with self.lock:
            entry = self.queries.get(normalize_query(query))
            return list(entry.get("video_ids", [])) if entry else []

    def add_query(self, query: str, max_results: int, video_ids: list[str]) -> None:
        with self.lock:
            key = normalize_query(query)
            existing = self.queries.get(key, {})
            merged_ids = list(dict.fromkeys(list(existing.get("video_ids", [])) + video_ids))
            self.queries[key] = {
                "max_results": max(max_results, existing.get("max_results", 0)),
                "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "video_ids": merged_ids,
            }
            self._dirty = True

    # -- videos -------------------------------------------------------------
    def get_video(self, video_id: str) -> dict | None:
        with self.lock:
            entry = self.videos.get(video_id)
            return dict(entry) if entry else None

    def add_video(self, video_id: str, data: dict) -> None:
        with self.lock:
            self.videos[video_id] = data
            self._dirty = True

    # -- domains ------------------------------------------------------------
    def get_domain(self, domain: str) -> dict | None:
        with self.lock:
            entry = self.domains.get(domain)
            return dict(entry) if entry else None

    def add_domain(self, domain: str, data: dict) -> None:
        with self.lock:
            self.domains[domain] = data
            self._dirty = True

    # -- rows ---------------------------------------------------------------
    def upsert_row(self, video_id: str, domain: str, row: dict) -> None:
        with self.lock:
            self.rows[_row_key(video_id, domain)] = row
            self._dirty = True

    def all_rows(self) -> list[dict]:
        with self.lock:
            return list(self.rows.values())

    def available_row_keys(self) -> set[str]:
        with self.lock:
            return {k for k, v in self.rows.items() if v.get("status") == "available"}

    # -- persistence --------------------------------------------------------
    def flush(self) -> None:
        with self.lock:
            if not self._dirty:
                return
            _atomic_write_json(self.queries_path, self.queries)
            _atomic_write_json(self.videos_path, self.videos)
            _atomic_write_json(self.domains_path, self.domains)
            _atomic_write_json(self.rows_path, self.rows)
            self._dirty = False


# ---------------------------------------------------------------------------
# YouTube search
# ---------------------------------------------------------------------------


_VIEW_RE = re.compile(r"[\d,]+")


def _parse_views(raw: Any) -> int:
    if raw is None:
        return 0
    if isinstance(raw, int):
        return raw
    if isinstance(raw, dict):
        for key in ("text", "short"):
            v = raw.get(key)
            if v:
                return _parse_views(v)
        return 0
    m = _VIEW_RE.search(str(raw))
    if not m:
        return 0
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return 0


def _description_text(video: dict) -> str:
    # youtube-search-python returns either "descriptionSnippet": [{"text": "..."}, ...]
    # or a plain string at "description". We accept both.
    snippet = video.get("descriptionSnippet")
    if isinstance(snippet, list):
        return " ".join(part.get("text", "") for part in snippet if isinstance(part, dict))
    if isinstance(snippet, str):
        return snippet
    desc = video.get("description")
    if isinstance(desc, str):
        return desc
    return ""


def search_videos(query: str, max_results: int, cache: Cache) -> list[dict]:
    """Return a list of {id, url, title, views, description} dicts for `query`.

    Cached: if `cache.has_query(query, max_results)`, the full set is reconstructed
    from `cache.videos` without any network call.
    """
    if cache.has_query(query, max_results):
        ids = cache.get_cached_video_ids(query)[:max_results]
        logging.info("[cache] query %r -> %d videos (no network)", query, len(ids))
        out = []
        for vid in ids:
            v = cache.get_video(vid)
            if v:
                out.append(
                    {
                        "id": vid,
                        "url": v.get("url", f"https://www.youtube.com/watch?v={vid}"),
                        "title": v.get("title", ""),
                        "views": int(v.get("views", 0)),
                        "description": v.get("description", ""),
                    }
                )
        return out

    logging.info("[net]   searching YouTube: %r (limit=%d)", query, max_results)
    try:
        search = VideosSearch(query, limit=max_results)
        payload = search.result()
    except Exception as exc:  # noqa: BLE001
        logging.warning("YouTube search failed for %r: %s", query, exc)
        return []

    results = (payload or {}).get("result", []) or []
    out: list[dict] = []
    ids: list[str] = []
    for video in results:
        vid = video.get("id")
        if not vid:
            continue
        url = video.get("link") or f"https://www.youtube.com/watch?v={vid}"
        title = video.get("title", "") or ""
        views = _parse_views(video.get("viewCount"))
        description = _description_text(video)
        out.append(
            {
                "id": vid,
                "url": url,
                "title": title,
                "views": views,
                "description": description,
            }
        )
        ids.append(vid)

    cache.add_query(query, max_results, ids)
    return out


# ---------------------------------------------------------------------------
# Domain extraction + availability
# ---------------------------------------------------------------------------


_URL_RE = re.compile(
    r"https?://[^\s<>\"'\)\]\}]+|(?<![\w@])www\.[\w.-]+\.[a-z]{2,}(?:/[^\s<>\"'\)\]\}]*)?",
    re.IGNORECASE,
)

# tldextract uses a bundled PSL snapshot; disable the HTTP update so first-run
# behavior is fast and offline-friendly.
_tld = tldextract.TLDExtract(suffix_list_urls=())


def extract_domains(text: str) -> list[str]:
    if not text:
        return []
    seen: set[str] = set()
    for match in _URL_RE.finditer(text):
        candidate = match.group(0).rstrip(".,);]}>'\"")
        if candidate.lower().startswith("www."):
            candidate = "http://" + candidate
        ext = _tld(candidate)
        if not ext.domain or not ext.suffix:
            continue
        registrable = f"{ext.domain}.{ext.suffix}".lower()
        if registrable in IGNORED_DOMAINS:
            continue
        seen.add(registrable)
    return sorted(seen)


def _tld_pricing(domain: str) -> tuple[str, str, str]:
    tld = domain.rsplit(".", 1)[-1].lower()
    price, registrar, url_tmpl = TLD_PRICES.get(tld, DEFAULT_PRICE)
    return price, registrar, url_tmpl.format(domain)


def check_domain(domain: str, session: requests.Session) -> dict:
    """Ask rdap.org whether `domain` is registered.

    Returns a dict with keys: status, available, price, registrar, buy_url,
    checked_at. `status` is one of "available", "registered", "unknown".
    """
    url = RDAP_URL.format(domain)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rdap+json, application/json"}

    status = "unknown"
    available: bool | None = None
    for attempt in (1, 2):
        try:
            resp = session.get(url, headers=headers, timeout=RDAP_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            logging.debug("RDAP error for %s (attempt %d): %s", domain, attempt, exc)
            if attempt == 1:
                time.sleep(RDAP_RETRY_SLEEP)
                continue
            break

        if resp.status_code == 404:
            status, available = "available", True
            break
        if resp.status_code == 200:
            status, available = "registered", False
            break
        if resp.status_code == 429 and attempt == 1:
            time.sleep(RDAP_RETRY_SLEEP)
            continue
        logging.debug("RDAP %s -> HTTP %s", domain, resp.status_code)
        break

    price, registrar, buy_url = (
        _tld_pricing(domain) if available else ("", "", "")
    )
    return {
        "status": status,
        "available": available,
        "price": price,
        "registrar": registrar,
        "buy_url": buy_url,
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Run state + orchestration
# ---------------------------------------------------------------------------


class RunState:
    def __init__(self, cache: Cache, output_path: Path, flush_every: int) -> None:
        self.cache = cache
        self.output_path = output_path
        self.flush_every = flush_every
        self.lock = threading.RLock()
        self.seen_available: set[str] = cache.available_row_keys()
        self.expired_since_flush = 0
        self.domain_futures: list[Future] = []

    def record_available(self, row_key: str) -> None:
        with self.lock:
            if row_key in self.seen_available:
                return
            self.seen_available.add(row_key)
            self.expired_since_flush += 1
            if self.expired_since_flush >= self.flush_every:
                self.expired_since_flush = 0
                self._flush_locked()

    def _flush_locked(self) -> None:
        logging.info("[flush] writing CSV + cache (%d available rows total)",
                     len(self.seen_available))
        write_csv(self.output_path, self.cache)
        self.cache.flush()

    def final_flush(self) -> None:
        with self.lock:
            self._flush_locked()


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def resolve_domain(
    video: dict, domain: str, cache: Cache, state: RunState, session: requests.Session
) -> None:
    try:
        record = cache.get_domain(domain)
        if record is None:
            record = check_domain(domain, session)
            cache.add_domain(domain, record)
            logging.info(
                "[net]   rdap %s -> %s", domain, record.get("status")
            )
        else:
            logging.debug("[cache] domain %s -> %s", domain, record.get("status"))

        row = {
            "video_id": video["id"],
            "video_url": video["url"],
            "video_title": video["title"],
            "views": int(video.get("views") or 0),
            "domain": domain,
            "status": record.get("status", "unknown"),
            "price": record.get("price") or "",
            "registrar": record.get("registrar") or "",
            "buy_url": record.get("buy_url") or "",
            "checked_at": record.get("checked_at", ""),
        }
        cache.upsert_row(video["id"], domain, row)
        if row["status"] == "available":
            state.record_available(_row_key(video["id"], domain))
    except Exception as exc:  # noqa: BLE001
        logging.warning("resolve_domain failed for %s: %s", domain, exc)


def process_video(
    video: dict,
    cache: Cache,
    state: RunState,
    pool: ThreadPoolExecutor,
    session: requests.Session,
) -> None:
    cached = cache.get_video(video["id"])
    if cached and "domains" in cached:
        domains = list(cached["domains"])
        logging.debug("[cache] video %s -> %d domains", video["id"], len(domains))
    else:
        domains = extract_domains(video.get("description", ""))
        cache.add_video(
            video["id"],
            {
                "url": video["url"],
                "title": video["title"],
                "views": int(video.get("views") or 0),
                "description": video.get("description", ""),
                "domains": domains,
                "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            },
        )
        logging.info(
            "[net]   video %s (%d views) -> %d domains",
            video["id"], video.get("views", 0), len(domains),
        )

    for domain in domains:
        fut = pool.submit(resolve_domain, video, domain, cache, state, session)
        with state.lock:
            state.domain_futures.append(fut)


# ---------------------------------------------------------------------------
# CSV rendering
# ---------------------------------------------------------------------------


def write_csv(path: Path, cache: Cache) -> None:
    rows = cache.all_rows()
    rows.sort(key=lambda r: (-int(r.get("views", 0) or 0), r.get("domain", "")))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def read_queries(path: Path) -> list[str]:
    lines: list[str] = []
    with path.open("r", encoding="utf-8") as fp:
        for raw in fp:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    # dedupe while preserving order
    return list(dict.fromkeys(lines))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape YouTube descriptions, report expired domains to CSV."
    )
    p.add_argument("--queries-file", required=True, type=Path,
                   help="Text file with one search query per line.")
    p.add_argument("--min-views", type=int, default=1000,
                   help="Skip videos with fewer than this many views (default: 1000).")
    p.add_argument("--max-results-per-query", type=int, default=50,
                   help="How many videos to pull per query (default: 50).")
    p.add_argument("--workers", type=int, default=10,
                   help="Thread pool size (default: 10).")
    p.add_argument("--cache-dir", type=Path, default=Path("./cache"),
                   help="Directory for JSON caches (default: ./cache).")
    p.add_argument("--output", type=Path, default=Path("./report.csv"),
                   help="Output CSV path (default: ./report.csv).")
    p.add_argument("--flush-every", type=int, default=FLUSH_EVERY_N_AVAILABLE,
                   help="Rewrite CSV after this many new available domains "
                        f"(default: {FLUSH_EVERY_N_AVAILABLE}).")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    return p.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.queries_file.exists():
        logging.error("Queries file not found: %s", args.queries_file)
        return 2

    queries = read_queries(args.queries_file)
    if not queries:
        logging.error("No queries found in %s", args.queries_file)
        return 2

    cache = Cache(args.cache_dir)
    state = RunState(cache, args.output, flush_every=max(1, args.flush_every))
    session = _session()

    logging.info(
        "Starting run: %d queries, min_views=%d, max_per_query=%d, workers=%d",
        len(queries), args.min_views, args.max_results_per_query, args.workers,
    )

    try:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            video_futures: list[Future] = []
            for query in queries:
                try:
                    videos = search_videos(query, args.max_results_per_query, cache)
                except Exception as exc:  # noqa: BLE001
                    logging.warning("search_videos(%r) failed: %s", query, exc)
                    continue
                for video in videos:
                    if int(video.get("views") or 0) < args.min_views:
                        continue
                    video_futures.append(
                        pool.submit(process_video, video, cache, state, pool, session)
                    )

            wait(video_futures)

            # Domain work is submitted from inside process_video; drain it too.
            while True:
                with state.lock:
                    pending = list(state.domain_futures)
                    state.domain_futures.clear()
                if not pending:
                    break
                wait(pending)
    finally:
        state.final_flush()
        logging.info(
            "Done. %d rows, %d available. CSV: %s",
            len(cache.all_rows()), len(state.seen_available), args.output,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
