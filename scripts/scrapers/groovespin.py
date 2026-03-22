# groovespinean.py (v8) — Wizard menu + verbose output per actie + test limits
#
# Usage:
#   python groovespinean.py
#     -> interactive wizard
#
# Optional non-interactive:
#   python groovespinean.py --mode test --headless true --limit-items 30 --limit-ean 30
#   python groovespinean.py --mode full --headless true --max-pages 500
#   python groovespinean.py --mode listing --headless true --limit-items 100
#   python groovespinean.py --mode ean-only --stage1-file output/some.stage1.csv --limit-ean 200
#
# Output:
#   output/groovespin_albums_<ts>.stage1.csv
#   output/groovespin_albums_<ts>.csv
#   output/groovespin_albums_<ts>.meta.json
#   output/ean_cache.json

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from _rotation import load_rotation_state, save_rotation_state, select_round_robin_batch

# -----------------------
# Defaults
# -----------------------

START_URL = (
    "https://www.groovespin.nl/albums"
    "?formats%5B%5D=1&formats%5B%5D=4&formats%5B%5D=7"
    "&sort_by=price-asc&bazar%5B%5D=0&availables%5B%5D=1"
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_MS = 45_000
DEFAULT_SLEEP = 0.05

EAN_WORKERS_DEFAULT = 24
EAN_TIMEOUT_S = 25
EAN_MAX_RETRIES = 3
EAN_BACKOFF = 1.6

BASE_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

_thread_local = threading.local()


# -----------------------
# Logging / Utils
# -----------------------

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        safe = line.encode('ascii', errors='replace').decode('ascii')
        print(safe, flush=True)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def normalize_ws(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def parse_artist_year(s: str) -> Tuple[str, Optional[int]]:
    s = normalize_ws(s)
    m = re.match(r"^(.*)\s+(\d{4})$", s)
    if not m:
        return s, None
    artist = normalize_ws(m.group(1))
    year = int(m.group(2))
    if 1900 <= year <= 2100:
        return artist, year
    return s, None


def parse_price_eur(raw: str) -> Tuple[str, Optional[float]]:
    t = normalize_ws(raw).replace("€", "").strip()
    raw_clean = t
    num = t.replace(".", "").replace(",", ".")
    try:
        return raw_clean, float(num)
    except Exception:
        return raw_clean, None


def set_page_param(url: str, page: int) -> str:
    """Preserve encoding/duplicate params; only replace/append page=<n>."""
    if re.search(r"([?&])page=\d+", url):
        return re.sub(r"([?&])page=\d+", rf"\1page={page}", url, count=1)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}page={page}"


def ensure_abs_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        return "https://www.groovespin.nl" + u
    return u


# -----------------------
# Playwright helpers (Stage 1)
# -----------------------

def dismiss_popups(page) -> None:
    # Best-effort cookie/modal dismissal
    selectors = [
        "button:has-text('Accepteer')",
        "button:has-text('Akkoord')",
        "button:has-text('Accept')",
        "button:has-text('Alles accepteren')",
        "button[aria-label*='close' i]",
        ".cookie button",
        ".cc-compliance button",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=700):
                loc.click(timeout=1200)
                log(f"[UI] dismissed popup via selector: {sel}")
                break
        except Exception:
            continue
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def install_seen_counter(page) -> None:
    # Tracks unique data-master-id count in window.__gs_total_seen
    page.evaluate(
        """
        (() => {
          window.__gs_seen_ids = window.__gs_seen_ids || new Set();
          window.__gs_total_seen = window.__gs_total_seen || 0;

          function scan() {
            const boxes = document.querySelectorAll('div.product-box[data-master-id]');
            for (const b of boxes) {
              const id = b.getAttribute('data-master-id');
              if (!id) continue;
              if (!window.__gs_seen_ids.has(id)) {
                window.__gs_seen_ids.add(id);
                window.__gs_total_seen += 1;
              }
            }
          }

          scan();

          if (!window.__gs_obs) {
            window.__gs_obs = new MutationObserver(() => scan());
            window.__gs_obs.observe(document.documentElement || document.body, { childList: true, subtree: true });
          }
        })();
        """
    )


def wait_products_ready(page, timeout_ms: int = DEFAULT_TIMEOUT_MS) -> None:
    page.wait_for_selector("div.product-box[data-master-id]", timeout=timeout_ms)
    page.wait_for_timeout(250)


def extract_listing_rows(page) -> List[Dict[str, Any]]:
    # Pull everything in one JS call (fast)
    rows = page.evaluate(
        """
        (() => {
          const out = [];
          const boxes = document.querySelectorAll('div.product-box[data-master-id]');
          for (const box of boxes) {
            const masterId = box.getAttribute('data-master-id') || '';
            const a = box.querySelector('.product-detail a[href]') || box.querySelector('a[href]');
            const url = a ? a.getAttribute('href') : '';

            const titleEl = box.querySelector('.product-detail-title');
            const title = titleEl ? titleEl.textContent : '';

            const metaEl = box.querySelector('.product-detail h5.mb-0')
                      || box.querySelector('.product-detail h5')
                      || box.querySelector('h5');
            const meta = metaEl ? metaEl.textContent : '';

            let priceText = '';
            const h4s = box.querySelectorAll('.product-detail h4');
            for (const h of h4s) {
              const t = (h.textContent || '');
              if (t.includes('€')) priceText = t;
            }
            out.push({ master_id: masterId, url, title, meta, price_text: priceText });
          }
          return out;
        })();
        """
    )

    parsed: List[Dict[str, Any]] = []
    for r in rows:
        master_id = normalize_ws(r.get("master_id"))
        url = normalize_ws(r.get("url"))
        title = normalize_ws(r.get("title"))
        meta = normalize_ws(r.get("meta"))
        price_text = normalize_ws(r.get("price_text"))

        artist, year = parse_artist_year(meta)
        price_raw, price_float = parse_price_eur(price_text)

        parsed.append(
            {
                "master_id": master_id,
                "url": ensure_abs_url(url),
                "artist": artist,
                "title": title,
                "year": year if year is not None else "",
                "price_raw": price_raw,
                "price_eur": f"{price_float:.2f}" if isinstance(price_float, float) else "",
            }
        )
    return parsed


def try_click_loadmore(page, timeout_ms: int = DEFAULT_TIMEOUT_MS) -> bool:
    install_seen_counter(page)
    prev_seen = int(page.evaluate("window.__gs_total_seen || 0"))

    candidates = [
        "button.loadmore",
        "button:has-text('Meer weergeven')",
        ".loadmore",
        "button.btn:has-text('Meer weergeven')",
    ]

    btn = None
    for sel in candidates:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0 and loc.is_visible(timeout=900):
                btn = loc
                break
        except Exception:
            continue

    if btn is None:
        log("[STAGE1] loadmore button not visible (selectors miss).")
        return False

    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        page.wait_for_timeout(200)
        btn.scroll_into_view_if_needed(timeout=2000)
        btn.click(timeout=timeout_ms, force=True)
        log("[STAGE1] action=CLICK_LOADMORE performed.")
    except Exception as e:
        log(f"[STAGE1] action=CLICK_LOADMORE failed: {type(e).__name__}: {e}")
        return False

    try:
        page.wait_for_function(f"(window.__gs_total_seen || 0) > {prev_seen}", timeout=timeout_ms)
        return True
    except PlaywrightTimeoutError:
        log("[STAGE1] action=CLICK_LOADMORE no new products detected.")
        return False


def try_infinite_scroll(page, steps: int = 3) -> bool:
    install_seen_counter(page)
    prev_seen = int(page.evaluate("window.__gs_total_seen || 0"))

    log(f"[STAGE1] action=SCROLL steps={steps}")
    for _ in range(steps):
        try:
            page.mouse.wheel(0, 2600)
        except Exception:
            page.evaluate("window.scrollBy(0, 2600);")
        page.wait_for_timeout(250)
        cur_seen = int(page.evaluate("window.__gs_total_seen || 0"))
        if cur_seen > prev_seen:
            return True

    log("[STAGE1] action=SCROLL no new products detected.")
    return False


# -----------------------
# Stage 2 (EAN) helpers
# -----------------------

def get_thread_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(BASE_HEADERS)
        _thread_local.session = s
    return s


def extract_ean_from_html(html: str) -> Optional[str]:
    if not html:
        return None

    # Prefer "Details" region where the page shows EAN
    try:
        import lxml.html  # type: ignore
        doc = lxml.html.fromstring(html)
        text = " ".join(doc.text_content().split())
        low = text.lower()

        idx = low.find("details")
        segment = text[idx: idx + 6000] if idx != -1 else text

        m = re.search(r"\bEAN\b\s*[: ]\s*(\d{8,14})\b", segment)
        if m:
            return m.group(1)

        m2 = re.search(r"\bEAN\b\s*[: ]\s*(\d{8,14})\b", text)
        if m2:
            return m2.group(1)
    except Exception:
        pass

    # Fallback regex
    m3 = re.search(r"\bEAN\b[^0-9]{0,30}(\d{8,14})\b", html)
    if m3:
        return m3.group(1)

    return None


def fetch_ean(url: str, timeout_s: int = EAN_TIMEOUT_S) -> Tuple[str, Optional[str], Optional[str], float]:
    s = get_thread_session()
    last_err: Optional[str] = None

    t0 = time.perf_counter()
    for attempt in range(1, EAN_MAX_RETRIES + 1):
        try:
            r = s.get(url, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            ean = extract_ean_from_html(r.text or "")
            elapsed = time.perf_counter() - t0
            return (url, ean, None, elapsed)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < EAN_MAX_RETRIES:
                time.sleep((EAN_BACKOFF ** (attempt - 1)) * 0.4)

    elapsed = time.perf_counter() - t0
    return (url, None, last_err, elapsed)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


# -----------------------
# Core: Stage1 / Stage2
# -----------------------

@dataclass
class RunConfig:
    mode: str  # test|full|listing|ean-only
    url: str
    out_dir: Path
    headless: bool
    sleep: float

    # Stage1 limits
    max_clicks: int = 0
    max_items: int = 0
    max_pages: int = 0  # pagination fallback cap

    # Stage2
    with_ean: bool = True
    ean_workers: int = EAN_WORKERS_DEFAULT
    ean_cache: Optional[Path] = None
    ean_log_cache_hits: bool = True
    limit_ean: int = 0  # test cap for stage2

    # ean-only input
    stage1_file: Optional[Path] = None
    ean_rotation_state: Optional[Path] = None


def stage1_scrape(cfg: RunConfig, stage1_csv: Path) -> Tuple[int, Dict[str, str]]:
    """
    Returns:
      - unique_count
      - url_label map for stage2 logging
    """
    seen_master = set()
    url_label: Dict[str, str] = {}

    log(f"[STAGE1] start headless={cfg.headless} max_items={cfg.max_items or 'inf'} max_pages={cfg.max_pages or 'inf'}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=cfg.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=UA,
            locale="nl-NL",
            viewport={"width": 1400, "height": 900},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        # Block heavy assets (keep CSS: helps visibility/clickability)
        def route_handler(route):
            rtype = route.request.resource_type
            if rtype in ("image", "media", "font"):
                return route.abort()
            return route.continue_()

        context.route("**/*", route_handler)
        page = context.new_page()

        log(f"[STAGE1] opening url={cfg.url}")
        page.goto(cfg.url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
        dismiss_popups(page)
        wait_products_ready(page)

        with stage1_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["timestamp", "master_id", "url", "artist", "title", "year", "price_raw", "price_eur"],
                delimiter="~",
            )
            w.writeheader()

            def flush_new(tag: str) -> int:
                rows = extract_listing_rows(page)
                new_n = 0
                for r in rows:
                    mid = r.get("master_id") or ""
                    if not mid or mid in seen_master:
                        continue
                    seen_master.add(mid)

                    # build label for stage2
                    u = r.get("url") or ""
                    artist = (r.get("artist") or "").strip()
                    title = (r.get("title") or "").strip()
                    year = (str(r.get("year") or "")).strip()
                    label = f"{artist} | {title}" + (f" ({year})" if year else "")
                    url_label[u] = label

                    out = {"timestamp": datetime.now().isoformat(timespec="seconds"), **r}
                    w.writerow(out)
                    new_n += 1

                    if cfg.max_items and len(seen_master) >= cfg.max_items:
                        break

                if new_n:
                    f.flush()
                log(f"[STAGE1] {tag} parsed_tiles={len(rows)} new={new_n} total_unique={len(seen_master)}")
                return new_n

            # Initial
            flush_new("INIT")

            if cfg.max_items and len(seen_master) >= cfg.max_items:
                log("[STAGE1] stop: max_items reached after INIT.")
            else:
                # Phase A/B: loadmore / scroll loop (if available)
                clicks = 0
                stuck = 0
                while True:
                    if cfg.max_items and len(seen_master) >= cfg.max_items:
                        log("[STAGE1] stop: max_items reached.")
                        break
                    if cfg.max_clicks and clicks >= cfg.max_clicks:
                        log("[STAGE1] stop: max_clicks reached.")
                        break

                    dismiss_popups(page)

                    ok = try_click_loadmore(page)
                    if not ok:
                        ok = try_infinite_scroll(page, steps=3)

                    flush_new(f"LOAD#{clicks+1}")

                    if ok:
                        stuck = 0
                    else:
                        stuck += 1

                    clicks += 1
                    time.sleep(cfg.sleep)

                    if stuck >= 2:
                        log("[STAGE1] loadmore/scroll inactive (2x) -> switching to pagination fallback.")
                        break

                # Phase C: pagination fallback
                page_idx = 2
                while True:
                    if cfg.max_items and len(seen_master) >= cfg.max_items:
                        log("[STAGE1] stop: max_items reached.")
                        break
                    if cfg.max_pages and page_idx > cfg.max_pages:
                        log(f"[STAGE1] stop: max_pages reached: {cfg.max_pages}")
                        break

                    next_url = set_page_param(cfg.url, page_idx)
                    log(f"[STAGE1] action=PAGINATION goto page={page_idx}")

                    try:
                        page.goto(next_url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
                    except PlaywrightTimeoutError:
                        log(f"[STAGE1] WARN timeout on goto page={page_idx}, continuing.")

                    dismiss_popups(page)
                    try:
                        wait_products_ready(page)
                    except Exception:
                        log(f"[STAGE1] page={page_idx} no products -> stopping pagination.")
                        break

                    new_n = flush_new(f"PAGE{page_idx}")

                    if new_n == 0:
                        log("[STAGE1] stop: no new items on next page.")
                        break

                    page_idx += 1
                    time.sleep(cfg.sleep)

        context.close()
        browser.close()

    log(f"[STAGE1] done unique_items={len(seen_master)} -> {stage1_csv}")
    return len(seen_master), url_label


def stage2_enrich_ean(
    cfg: RunConfig,
    stage1_csv: Path,
    final_csv: Path,
    url_label: Optional[Dict[str, str]] = None,
) -> Tuple[int, int, int]:
    """
    Returns: (urls_total, ok, err)
    """
    cache_path = cfg.ean_cache or (cfg.out_dir / "ean_cache.json")
    cache = load_json(cache_path)

    # Read URLs from input file. If an existing EAN column is present, preserve it and only fetch missing EANs.
    urls: List[str] = []
    labels: Dict[str, str] = url_label.copy() if url_label else {}
    existing_eans: Dict[str, str] = {}

    with stage1_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="~")
        for row in r:
            u = ensure_abs_url(row.get("url") or "")
            if not u:
                continue
            if u not in urls:
                urls.append(u)

            existing_ean = normalize_ws(row.get("ean") or "")
            if existing_ean and u not in existing_eans:
                existing_eans[u] = existing_ean

            if u not in labels:
                artist = (row.get("artist") or "").strip()
                title = (row.get("title") or "").strip()
                year = (row.get("year") or "").strip()
                labels[u] = f"{artist} | {title}" + (f" ({year})" if year else "")

    # limit_ean applies to records without an existing EAN. Rotate through that backlog so
    # repeated runs do not keep hitting the same first N URLs.
    fetch_candidates = [u for u in urls if not existing_eans.get(u)]
    if cfg.limit_ean:
        rotation_state_path = cfg.ean_rotation_state or (cfg.out_dir / "groovespin_ean_rotation_state.json")
        rotation_state = load_rotation_state(rotation_state_path)
        fetch_candidates = select_round_robin_batch(fetch_candidates, cfg.limit_ean, rotation_state, "missing_ean_urls")
        save_rotation_state(rotation_state_path, rotation_state)
        allowed = set(existing_eans) | set(fetch_candidates)
        urls = [u for u in urls if u in allowed]
        log(
            f"[STAGE2] limit_ean applied with rotation -> urls={len(urls)} "
            f"fetch_candidates={len(fetch_candidates)} state={rotation_state_path}"
        )

    # Seed cache with already known EANs from the input file so they are preserved and skipped.
    preserved = 0
    for u, ean in existing_eans.items():
        if u in urls and ean:
            if cache.get(u) != ean:
                cache[u] = ean
            preserved += 1

    # Log cache hits (optional)
    cache_hits = [u for u in urls if cache.get(u)]
    to_fetch = [u for u in fetch_candidates if not cache.get(u)]

    if cfg.ean_log_cache_hits and cache_hits:
        show = cache_hits[:25]
        for i, u in enumerate(show, start=1):
            origin = 'preserved' if existing_eans.get(u) else 'cache'
            log(f"[EAN:CACHE] ({i}/{len(cache_hits)}) [{origin}] {labels.get(u,'')} -> {cache.get(u)}")
        if len(cache_hits) > len(show):
            log(f"[EAN:CACHE] ... +{len(cache_hits)-len(show)} more cache hits")

    log(f"[STAGE2] urls={len(urls)} preserved={preserved} to_fetch={len(to_fetch)} cache_hits={len(cache_hits)} workers={cfg.ean_workers}")

    ok = 0
    err = 0

    if to_fetch:
        workers = max(4, int(cfg.ean_workers))
        t0 = time.perf_counter()

        done = 0
        total = len(to_fetch)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(fetch_ean, u, EAN_TIMEOUT_S): u for u in to_fetch}
            for fut in as_completed(futures):
                u, ean, e, elapsed = fut.result()
                done += 1

                if ean:
                    cache[u] = ean
                    ok += 1
                    log(f"[EAN] ({done}/{total}) [OK] {labels.get(u,'')} -> {ean} | {elapsed:.2f}s")
                else:
                    cache.setdefault(u, "")
                    err += 1
                    log(f"[EAN] ({done}/{total}) [ERR] {labels.get(u,'')} | {elapsed:.2f}s | {e}")

        save_json(cache_path, cache)
        log(f"[STAGE2] completed in {(time.perf_counter()-t0):.2f}s | ok={ok} err={err} cache={cache_path}")
    else:
        log("[STAGE2] nothing to fetch (all in cache).")

    # Write final CSV (stage1 + ean)
    with stage1_csv.open("r", encoding="utf-8", newline="") as fin, final_csv.open("w", encoding="utf-8", newline="") as fout:
        r = csv.DictReader(fin, delimiter="~")
        fieldnames = list(r.fieldnames or [])
        if "ean" not in fieldnames:
            fieldnames.append("ean")
        w = csv.DictWriter(fout, fieldnames=fieldnames, delimiter="~")
        w.writeheader()

        allowed = set(urls)  # if limit_ean was set, only output those
        for row in r:
            u = ensure_abs_url(row.get("url") or "")
            if cfg.limit_ean and u not in allowed:
                continue
            row["ean"] = cache.get(u, "") or ""
            w.writerow(row)

    log(f"[STAGE2] final saved -> {final_csv}")
    return len(urls), ok, err


# -----------------------
# Wizard / Menu
# -----------------------

def prompt_bool(q: str, default: bool) -> bool:
    d = "Y" if default else "N"
    while True:
        s = input(f"{q} [{d}/{'N' if default else 'Y'}]: ").strip().lower()
        if not s:
            return default
        if s in ("y", "yes", "j", "ja", "1", "true"):
            return True
        if s in ("n", "no", "nee", "0", "false"):
            return False
        print("Typ y/n.")


def prompt_int(q: str, default: int, allow_zero: bool = True) -> int:
    while True:
        s = input(f"{q} [{default}]: ").strip()
        if not s:
            return default
        try:
            v = int(s)
            if v < 0:
                print("Moet >= 0 zijn.")
                continue
            if v == 0 and not allow_zero:
                print("0 is niet toegestaan.")
                continue
            return v
        except ValueError:
            print("Typ een integer.")


def prompt_float(q: str, default: float) -> float:
    while True:
        s = input(f"{q} [{default}]: ").strip()
        if not s:
            return default
        try:
            return float(s)
        except ValueError:
            print("Typ een getal (bijv 0.02).")


def find_stage1_files(out_dir: Path, limit: int = 10) -> List[Path]:
    if not out_dir.exists():
        return []
    files = sorted(out_dir.glob("groovespin_albums_*.stage1.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:limit]


def wizard() -> RunConfig:
    print()
    print("=== Groovespin Scraper Wizard ===")
    print("1) Test run (default: 30 items + 30 EANs)")
    print("2) Full scrape (alles) + EAN")
    print("3) Alleen listing (Stage 1)")
    print("4) Alleen EAN enrich (Stage 2 op bestaande stage1.csv)")
    print("5) Exit")
    print()

    choice = prompt_int("Kies optie", 1, allow_zero=False)

    if choice == 5:
        sys.exit(0)

    out_dir = Path("output")
    safe_mkdir(out_dir)

    headless = prompt_bool("Headless draaien?", True)
    sleep = prompt_float("Sleep tussen acties (sec)", 0.02 if choice == 1 else 0.05)
    workers = prompt_int("EAN workers (parallel requests)", EAN_WORKERS_DEFAULT)

    url = START_URL
    if choice in (1, 2, 3):
        # Let the user override URL if needed
        s = input(f"Start URL (enter = default) [{START_URL}]: ").strip()
        if s:
            url = s

    cfg = RunConfig(
        mode="test" if choice == 1 else ("full" if choice == 2 else ("listing" if choice == 3 else "ean-only")),
        url=url,
        out_dir=out_dir,
        headless=headless,
        sleep=sleep,
        ean_workers=workers,
        ean_cache=out_dir / "ean_cache.json",
        ean_rotation_state=out_dir / "groovespin_ean_rotation_state.json",
    )

    if choice == 1:
        cfg.max_items = prompt_int("Limit items (Stage1)", 30)
        cfg.max_pages = prompt_int("Limit pages (pagination fallback, 0=unlimited)", 3)
        cfg.limit_ean = prompt_int("Limit EAN fetches (Stage2)", cfg.max_items or 30)
        cfg.with_ean = True

    elif choice == 2:
        cfg.max_items = prompt_int("Max items (0=unlimited)", 0)
        cfg.max_pages = prompt_int("Max pages (0=unlimited, aanbevolen 500 als guardrail)", 500)
        cfg.limit_ean = 0
        cfg.with_ean = True

    elif choice == 3:
        cfg.max_items = prompt_int("Limit items (0=unlimited)", 100)
        cfg.max_pages = prompt_int("Max pages (0=unlimited)", 10)
        cfg.with_ean = False

    elif choice == 4:
        # Select a stage1 file
        files = find_stage1_files(out_dir)
        if not files:
            print("Geen stage1 files gevonden in ./output. Run eerst Stage 1.")
            sys.exit(1)

        print("\nKies stage1 bestand:")
        for idx, p in enumerate(files, start=1):
            print(f"{idx}) {p.name}")
        pick = prompt_int("Nummer", 1, allow_zero=False)
        pick = max(1, min(pick, len(files)))

        cfg.stage1_file = files[pick - 1]
        cfg.with_ean = True
        cfg.limit_ean = prompt_int("Limit EAN fetches (0=unlimited)", 100)
        # for ean-only we don't need these, but keep sane:
        cfg.max_pages = 0
        cfg.max_items = 0

    # Cache logging
    cfg.ean_log_cache_hits = prompt_bool("Stage2: cache hits ook printen?", True)

    print()
    return cfg


# -----------------------
# CLI parsing (non-interactive)
# -----------------------

def parse_args(argv: List[str]) -> Optional[RunConfig]:
    if not argv:
        return None  # wizard

    # Simple key/value parsing
    args: Dict[str, Any] = {
        "mode": "test",
        "url": START_URL,
        "out": "output",
        "headless": True,
        "sleep": DEFAULT_SLEEP,
        "max_clicks": 0,
        "max_items": 0,
        "max_pages": 0,
        "with_ean": True,
        "ean_workers": EAN_WORKERS_DEFAULT,
        "ean_cache": "",
        "ean_log_cache_hits": True,
        "limit_ean": 0,
        "stage1_file": "",
        "ean_rotation_state": "",
    }

    i = 0
    while i < len(argv):
        a = argv[i].strip()
        def pop() -> str:
            nonlocal i
            if i + 1 >= len(argv):
                raise ValueError(f"Missing value after {a}")
            i += 1
            return argv[i].strip()

        if a == "--mode":
            args["mode"] = pop()
        elif a == "--url":
            args["url"] = pop()
        elif a in ("--out", "--out-dir"):
            args["out"] = pop()
        elif a == "--headless":
            v = pop().lower()
            args["headless"] = v in ("1", "true", "yes", "y", "ja", "j")
        elif a == "--sleep":
            args["sleep"] = float(pop())
        elif a == "--max-clicks":
            args["max_clicks"] = int(pop())
        elif a in ("--max-items", "--limit-items"):
            args["max_items"] = int(pop())
        elif a in ("--max-pages", "--limit-pages"):
            args["max_pages"] = int(pop())
        elif a == "--with-ean":
            v = pop().lower()
            args["with_ean"] = v in ("1", "true", "yes", "y", "ja", "j")
        elif a == "--ean-workers":
            args["ean_workers"] = int(pop())
        elif a == "--ean-cache":
            args["ean_cache"] = pop()
        elif a == "--ean-log-cache-hits":
            v = pop().lower()
            args["ean_log_cache_hits"] = v in ("1", "true", "yes", "y", "ja", "j")
        elif a == "--limit-ean":
            args["limit_ean"] = int(pop())
        elif a == "--stage1-file":
            args["stage1_file"] = pop()
        elif a == "--ean-rotation-state":
            args["ean_rotation_state"] = pop()
        elif a in ("-h", "--help"):
            print(
                "python groovespinean.py [--mode test|full|listing|ean-only] [options]\n"
                "  --url <start_url>\n"
                "  --out <dir>\n"
                "  --headless <true|false>\n"
                "  --sleep <seconds>\n"
                "  --max-items <n> / --limit-items <n>\n"
                "  --max-pages <n> / --limit-pages <n>\n"
                "  --with-ean <true|false>\n"
                "  --ean-workers <n>\n"
                "  --ean-cache <path.json>\n"
                "  --ean-log-cache-hits <true|false>\n"
                "  --limit-ean <n>\n"
                "  --stage1-file <path> (for mode ean-only)\n"
                "  --ean-rotation-state <path.json>\n"
            )
            sys.exit(0)
        else:
            raise ValueError(f"Unknown arg: {a}")
        i += 1

    out_dir = Path(args["out"])
    safe_mkdir(out_dir)

    cfg = RunConfig(
        mode=str(args["mode"]).strip().lower(),
        url=str(args["url"]).strip(),
        out_dir=out_dir,
        headless=bool(args["headless"]),
        sleep=float(args["sleep"]),
        max_clicks=int(args["max_clicks"]),
        max_items=int(args["max_items"]),
        max_pages=int(args["max_pages"]),
        with_ean=bool(args["with_ean"]),
        ean_workers=int(args["ean_workers"]),
        ean_cache=Path(args["ean_cache"]) if args["ean_cache"] else (out_dir / "ean_cache.json"),
        ean_log_cache_hits=bool(args["ean_log_cache_hits"]),
        limit_ean=int(args["limit_ean"]),
        stage1_file=Path(args["stage1_file"]) if args["stage1_file"] else None,
        ean_rotation_state=Path(args["ean_rotation_state"]) if args["ean_rotation_state"] else (out_dir / "groovespin_ean_rotation_state.json"),
    )
    return cfg


# -----------------------
# Main orchestration
# -----------------------

def main() -> None:
    cfg = parse_args(sys.argv[1:])
    if cfg is None:
        cfg = wizard()

    safe_mkdir(cfg.out_dir)

    run_id = now_stamp()
    stage1_csv = cfg.out_dir / f"groovespin_albums_{run_id}.stage1.csv"
    final_csv = cfg.out_dir / f"groovespin_albums_{run_id}.csv"
    meta_json = cfg.out_dir / f"groovespin_albums_{run_id}.meta.json"

    log(f"[RUN] mode={cfg.mode} out_dir={cfg.out_dir}")
    log(f"[RUN] headless={cfg.headless} sleep={cfg.sleep} max_items={cfg.max_items or 'inf'} max_pages={cfg.max_pages or 'inf'} limit_ean={cfg.limit_ean or 'inf'}")
    log(f"[RUN] cache={cfg.ean_cache}")

    started = datetime.now().isoformat(timespec="seconds")

    unique_items = 0
    url_label: Dict[str, str] = {}
    urls_total = 0
    ean_ok = 0
    ean_err = 0

    if cfg.mode in ("test", "full", "listing"):
        unique_items, url_label = stage1_scrape(cfg, stage1_csv)
        if cfg.mode == "listing" or not cfg.with_ean:
            log("[RUN] listing-only: skipping stage2.")
            # write final with empty ean column so output is consistent
            with stage1_csv.open("r", encoding="utf-8", newline="") as fin, final_csv.open("w", encoding="utf-8", newline="") as fout:
                r = csv.DictReader(fin, delimiter="~")
                fieldnames = list(r.fieldnames or [])
                if "ean" not in fieldnames:
                    fieldnames.append("ean")
                w = csv.DictWriter(fout, fieldnames=fieldnames, delimiter="~")
                w.writeheader()
                for row in r:
                    row["ean"] = ""
                    w.writerow(row)
            log(f"[RUN] final saved (ean empty) -> {final_csv}")
        else:
            urls_total, ean_ok, ean_err = stage2_enrich_ean(cfg, stage1_csv, final_csv, url_label)

    elif cfg.mode in ("ean-only", "ean", "stage2"):
        if not cfg.stage1_file or not cfg.stage1_file.exists():
            raise RuntimeError("mode ean-only requires --stage1-file <path> (existing .stage1.csv)")
        stage1_csv = cfg.stage1_file
        final_csv = cfg.out_dir / f"groovespin_albums_{run_id}.ean_only.csv"
        urls_total, ean_ok, ean_err = stage2_enrich_ean(cfg, stage1_csv, final_csv, None)

    else:
        raise ValueError(f"Unknown mode: {cfg.mode}")

    ended = datetime.now().isoformat(timespec="seconds")
    meta = {
        "started_at": started,
        "ended_at": ended,
        "mode": cfg.mode,
        "url": cfg.url,
        "out_dir": str(cfg.out_dir),
        "stage1_csv": str(stage1_csv),
        "final_csv": str(final_csv),
        "unique_items_stage1": unique_items,
        "urls_stage2": urls_total,
        "ean_ok": ean_ok,
        "ean_err": ean_err,
        "headless": cfg.headless,
        "sleep": cfg.sleep,
        "max_items": cfg.max_items,
        "max_pages": cfg.max_pages,
        "limit_ean": cfg.limit_ean,
        "ean_workers": cfg.ean_workers,
        "ean_cache": str(cfg.ean_cache) if cfg.ean_cache else "",
        "ean_log_cache_hits": cfg.ean_log_cache_hits,
    }
    meta_json.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"[RUN] meta -> {meta_json}")
    log("[DONE]")


if __name__ == "__main__":
    main()