#!/usr/bin/env python3
from __future__ import annotations

import os
import py_compile
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


PATCH_MARKER = "# VINYLOFY_SHIPPING_SHOP_ID_RESOLUTION_V1"


def fail(message: str, code: int = 1) -> None:
    print(f"\n[ERROR] {message}", file=sys.stderr, flush=True)
    raise SystemExit(code)


def run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=capture,
        check=False,
    )


def main() -> int:
    probe = run(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd())
    if probe.returncode != 0:
        fail("voer dit script binnen de Vinylofy-repository uit")

    root = Path(probe.stdout.strip()).resolve()
    origin_result = run(["git", "remote", "get-url", "origin"], cwd=root)
    origin = origin_result.stdout.strip() if origin_result.returncode == 0 else ""

    allowed_origins = {
        "https://github.com/Vinylofy/Vinylofy",
        "https://github.com/Vinylofy/Vinylofy.git",
        "git@github.com:Vinylofy/Vinylofy.git",
    }
    if origin not in allowed_origins:
        fail(f"onverwachte origin: {origin or '<geen>'}")

    target = root / "scripts/tools/import_shipping_rules.py"
    if not target.exists():
        fail(f"shippingimporter ontbreekt: {target}")

    print("== Repository ==")
    print("root:  ", root)
    print("origin:", origin)

    status = run(["git", "status", "--short"], cwd=root)
    print("\n== Gitstatus vóór patch ==")
    print(status.stdout.rstrip() or "(schoon)")

    original = target.read_text(encoding="utf-8")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = Path("/tmp") / f"import_shipping_rules.py.before-shop-id-fix-{timestamp}"
    shutil.copy2(target, backup)
    print("\nBackup:", backup)

    if PATCH_MARKER in original:
        print("\n[INFO] shop_id-resolver is al aanwezig; codepatch overgeslagen.")
    else:
        pattern = re.compile(
            r"^(?P<indent>[ \t]*)cur\.executemany\(sql,\s*payload\)\s*$",
            flags=re.MULTILINE,
        )
        matches = list(pattern.finditer(original))
        if len(matches) != 1:
            fail(
                "verwachtte exact één 'cur.executemany(sql, payload)', "
                f"vond {len(matches)}; niets aangepast"
            )

        indent = matches[0].group("indent")
        block_lines = [
            PATCH_MARKER,
            "import re as _shipping_re",
            "from urllib.parse import urlparse as _shipping_urlparse",
            "",
            "_sql_text = str(sql)",
            "_insert_match = _shipping_re.search(",
            "    r\"insert\\s+into\\s+(?:public\\.)?shop_shipping_rules\\s*\"",
            "    r\"\\((?P<columns>.*?)\\)\\s*values\",",
            "    _sql_text,",
            "    flags=_shipping_re.IGNORECASE | _shipping_re.DOTALL,",
            ")",
            "if _insert_match is None:",
            "    raise RuntimeError(",
            "        \"Kan kolomvolgorde van shop_shipping_rules INSERT niet bepalen\"",
            "    )",
            "",
            "_insert_columns = [",
            "    part.strip().strip('\"').split('.')[-1]",
            "    for part in _insert_match.group('columns').split(',')",
            "]",
            "_required_columns = {'shop_id', 'shop_slug', 'shop_name', 'source_url'}",
            "_missing_columns = sorted(_required_columns - set(_insert_columns))",
            "if _missing_columns:",
            "    raise RuntimeError(",
            "        \"Shipping INSERT mist vereiste kolommen: \"",
            "        + \", \".join(_missing_columns)",
            "    )",
            "",
            "_shop_id_index = _insert_columns.index('shop_id')",
            "_shop_slug_index = _insert_columns.index('shop_slug')",
            "_shop_name_index = _insert_columns.index('shop_name')",
            "_source_url_index = _insert_columns.index('source_url')",
            "",
            "cur.execute(\"select id, name, domain from public.shops\")",
            "_shop_rows = cur.fetchall()",
            "",
            "def _normalize_shipping_domain(value):",
            "    value = str(value or '').strip().lower()",
            "    value = _shipping_re.sub(r'^https?://', '', value)",
            "    value = value.split('/', 1)[0].split(':', 1)[0]",
            "    if value.startswith('www.'):",
            "        value = value[4:]",
            "    return value",
            "",
            "def _normalize_shipping_name(value):",
            "    return _shipping_re.sub(",
            "        r'\\s+', ' ', str(value or '').strip().lower()",
            "    )",
            "",
            "_shops_by_domain = {}",
            "_shops_by_name = {}",
            "for _shop_id, _shop_name, _shop_domain in _shop_rows:",
            "    _domain_key = _normalize_shipping_domain(_shop_domain)",
            "    if _domain_key:",
            "        _shops_by_domain[_domain_key] = _shop_id",
            "    _name_key = _normalize_shipping_name(_shop_name)",
            "    if _name_key:",
            "        _shops_by_name.setdefault(_name_key, []).append(_shop_id)",
            "",
            "_resolved_payload = []",
            "for _payload_row in payload:",
            "    _values = list(_payload_row)",
            "    if _values[_shop_id_index] is None:",
            "        _source_url = str(_values[_source_url_index] or '').strip()",
            "        _source_host = _normalize_shipping_domain(",
            "            _shipping_urlparse(_source_url).hostname",
            "            if _source_url",
            "            else ''",
            "        )",
            "        _resolved_shop_id = _shops_by_domain.get(_source_host)",
            "",
            "        if _resolved_shop_id is None:",
            "            _shop_name_key = _normalize_shipping_name(",
            "                _values[_shop_name_index]",
            "            )",
            "            _name_matches = _shops_by_name.get(_shop_name_key, [])",
            "            if len(_name_matches) == 1:",
            "                _resolved_shop_id = _name_matches[0]",
            "",
            "        if _resolved_shop_id is None:",
            "            raise RuntimeError(",
            "                \"Geen unieke public.shops-koppeling voor shippingregel: \"",
            "                f\"shop_slug={_values[_shop_slug_index]!r}, \"",
            "                f\"shop_name={_values[_shop_name_index]!r}, \"",
            "                f\"source_url={_source_url!r}\"",
            "            )",
            "",
            "        _values[_shop_id_index] = _resolved_shop_id",
            "",
            "    _resolved_payload.append(tuple(_values))",
            "",
            "payload = _resolved_payload",
            "cur.executemany(sql, payload)",
        ]

        replacement = "\n".join(
            f"{indent}{line}" if line else ""
            for line in block_lines
        )

        patched = pattern.sub(replacement, original, count=1)
        if patched == original:
            fail("patch leverde geen wijziging op")

        target.write_text(patched, encoding="utf-8")
        print("\n[PATCH] generieke shop_id-resolutie toegevoegd.")

    try:
        py_compile.compile(str(target), doraise=True)
    except Exception as exc:
        shutil.copy2(backup, target)
        fail(f"compilecheck faalde; backup hersteld: {exc}")

    print("[OK] import_shipping_rules.py compileert.")

    help_result = run(
        [sys.executable, str(target), "--help"],
        cwd=root,
    )
    if help_result.returncode != 0:
        shutil.copy2(backup, target)
        fail(
            "CLI-check faalde; backup hersteld:\n"
            + (help_result.stderr or help_result.stdout)
        )
    print("[OK] shippingimporter CLI werkt.")

    shipping_files = sorted(
        Path("/tmp").glob("everythingjazz-shipping-only-*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not shipping_files:
        fail("geen tijdelijk Everything Jazz-shippingbestand gevonden")

    shipping_file = shipping_files[0]
    print("\n== Shippingimport ==")
    print("bestand:", shipping_file)

    sys.path.insert(0, str(root))
    from scripts.scrapers.usf.core.db import db_connection, get_database_url

    environment = os.environ.copy()
    environment["DATABASE_URL"] = get_database_url()

    import_result = run(
        [
            sys.executable,
            str(target),
            "--input",
            str(shipping_file),
        ],
        cwd=root,
        env=environment,
    )

    if import_result.stdout.strip():
        print(import_result.stdout.rstrip())
    if import_result.stderr.strip():
        print(import_result.stderr.rstrip(), file=sys.stderr)

    print("Import-exitcode:", import_result.returncode)
    if import_result.returncode != 0:
        fail(
            "shippingimport faalde; listing is niet opnieuw uitgevoerd. "
            f"Importerbackup: {backup}"
        )

    print("\n== Databasevalidatie ==")
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    ss.shop_id,
                    s.name,
                    s.domain,
                    ss.shop_slug,
                    ss.country_code,
                    ss.currency,
                    ss.shipping_cost_cents,
                    ss.free_shipping_threshold_cents,
                    ss.shipping_logic,
                    ss.active
                from public.shop_shipping_rules ss
                join public.shops s
                  on s.id = ss.shop_id
                where ss.shop_slug = 'everythingjazz'
                order by ss.active desc, ss.country_code
                """
            )
            rows = cursor.fetchall()

    for row in rows:
        print(
            {
                "shop_id": str(row[0]),
                "shop_name": row[1],
                "domain": row[2],
                "shop_slug": row[3],
                "country_code": row[4],
                "currency": row[5],
                "shipping_cost_cents": row[6],
                "free_shipping_threshold_cents": row[7],
                "shipping_logic": row[8],
                "active": row[9],
            }
        )

    exact = [
        row
        for row in rows
        if row[2] == "eustore.everythingjazz.com"
        and row[3] == "everythingjazz"
        and row[4] == "NL"
        and row[5] == "EUR"
        and row[6] == 995
        and row[7] is None
        and row[8] == "flat"
        and row[9] is True
    ]

    if len(exact) != 1:
        fail(
            "verwacht exact één actieve Everything Jazz-regel van "
            f"995 cent zonder gratis grens; gevonden: {len(exact)}"
        )

    diff_check = run(
        ["git", "diff", "--check", "--", str(target.relative_to(root))],
        cwd=root,
    )
    if diff_check.returncode != 0:
        fail("git diff --check faalde:\n" + diff_check.stdout + diff_check.stderr)

    print(
        "\n[OK] Everything Jazz-shipping is actief: "
        "€ 9,95 per bestelling, zonder gratis-verzenddrempel."
    )
    print("[OK] Generieke importer koppelt shop_id voortaan via bron-domein.")
    print("Backup:", backup)
    print("\n== Gewijzigd bestand ==")
    print(target.relative_to(root))
    print("\nNiets gecommit of gepusht.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[STOP] Afgebroken; terminal blijft beschikbaar.", file=sys.stderr)
        raise SystemExit(130)
