#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import re
import shutil
import sys


def replace_once(text: str, pattern: str, repl: str, description: str) -> str:
    new_text, count = re.subn(pattern, repl, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"Could not patch {description}; pattern not found exactly once.")
    return new_text


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch groovespin.py for importer compatibility and gentler EAN throttling.")
    parser.add_argument(
        "target",
        nargs="?",
        default="scripts/scrapers/groovespin.py",
        help="Path to groovespin.py inside the repo",
    )
    args = parser.parse_args()

    target = pathlib.Path(args.target)
    if not target.exists():
        raise SystemExit(f"Target file not found: {target}")

    original = target.read_text(encoding="utf-8")
    patched = original

    patched = replace_once(
        patched,
        r'def fetch_ean\(url: str, timeout_s: int = EAN_TIMEOUT_S\) -> Tuple\[str, Optional\[str\], Optional\[str\], float\]:\n(?:    .*\n)+?def load_json\(path: Path\) -> Dict\[str, Any\]:',
        '''def fetch_ean(url: str, timeout_s: int = EAN_TIMEOUT_S) -> Tuple[str, Optional[str], Optional[str], float]:
    s = get_thread_session()
    last_err: Optional[str] = None
    t0 = time.perf_counter()

    for attempt in range(1, EAN_MAX_RETRIES + 1):
        try:
            r = s.get(url, timeout=timeout_s)
            if r.status_code == 429:
                retry_after_raw = (r.headers.get("Retry-After") or "").strip()
                try:
                    retry_after = float(retry_after_raw) if retry_after_raw else 0.0
                except Exception:
                    retry_after = 0.0
                raise RuntimeError(f"HTTP 429|retry_after={retry_after}")
            if r.status_code in (500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")

            ean = extract_ean_from_html(r.text or "")
            elapsed = time.perf_counter() - t0
            return (url, ean, None, elapsed)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < EAN_MAX_RETRIES:
                wait_s = (EAN_BACKOFF ** (attempt - 1)) * 0.4
                m = re.search(r"retry_after=([0-9]+(?:\.[0-9]+)?)", str(e))
                if m:
                    try:
                        wait_s = max(wait_s, float(m.group(1)))
                    except Exception:
                        pass
                if "HTTP 429" in str(e):
                    wait_s = max(wait_s, 3.0 * attempt)
                time.sleep(wait_s)

    elapsed = time.perf_counter() - t0
    return (url, None, last_err, elapsed)


def load_json(path: Path) -> Dict[str, Any]:''',
        "fetch_ean()",
    )

    patched = replace_once(
        patched,
        r'with stage1_csv\.open\("r", encoding="utf-8", newline=""\) as fin, final_csv\.open\("w", encoding="utf-8", newline=""\) as fout:\n\s+r = csv\.DictReader\(fin, delimiter="~"\)\n\s+fieldnames = list\(r\.fieldnames or \[\]\)\n\s+if "ean" not in fieldnames:\n\s+\s+fieldnames\.append\("ean"\)\n\s+w = csv\.DictWriter\(fout, fieldnames=fieldnames, delimiter="~"\)\n\s+w\.writeheader\(\)',
        '''with stage1_csv.open("r", encoding="utf-8", newline="") as fin, final_csv.open("w", encoding="utf-8", newline="") as fout:
        r = csv.DictReader(fin, delimiter="~")
        fieldnames = list(r.fieldnames or [])
        if "ean" not in fieldnames:
            fieldnames.append("ean")
        w = csv.DictWriter(fout, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()''',
        "final CSV writer",
    )

    if patched == original:
        raise RuntimeError("No changes were applied.")

    backup = target.with_suffix(target.suffix + ".bak")
    shutil.copy2(target, backup)
    target.write_text(patched, encoding="utf-8")

    print(f"Patched: {target}")
    print(f"Backup : {backup}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
