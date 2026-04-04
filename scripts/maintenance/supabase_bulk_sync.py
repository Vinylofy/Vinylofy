from __future__ import annotations

import csv
import json
import os
import re
import tomllib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path, PureWindowsPath
from typing import Any, Callable

import pandas as pd

from core.db import create_run, update_run
from core.logging_utils import append_log, timestamp_slug
from core.parser import normalize_shop, run_parser_selection_export
from core.paths import EXPORTS_DIR, LOGS_DIR, ROOT_DIR
from core.scraper_registry import get_registry

try:
    import psycopg
except Exception as exc:  # pragma: no cover - runtime dependency path
    psycopg = None
    PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover - trivial
    PSYCOPG_IMPORT_ERROR = None

ProgressFn = Callable[[str], None] | None

PRODUCT_STAGE_COLUMNS = [
    'canonical_product_id',
    'ean',
    'artist',
    'title',
    'format',
    'label',
    'catalog_number',
    'release_year',
    'country',
    'cover_art_url',
    'musicbrainz_release_id',
    'metadata_source',
    'metadata_confidence',
    'first_seen_at',
    'last_seen_at',
    'upload_batch_id',
    'bundle_slug',
    'selected_shops',
    'loaded_at',
    'source_scope',
    'source_file',
    'validation_status',
    'validation_errors',
]

LATEST_STAGE_COLUMNS = [
    'canonical_product_id',
    'ean',
    'shop',
    'shop_url',
    'current_price',
    'currency',
    'availability',
    'snapshot_date',
    'snapshot_week',
    'last_scraped_at',
    'upload_batch_id',
    'bundle_slug',
    'selected_shops',
    'loaded_at',
    'source_scope',
    'source_file',
    'validation_status',
    'validation_errors',
]

HISTORY_STAGE_COLUMNS = [
    'canonical_product_id',
    'ean',
    'shop',
    'shop_url',
    'price',
    'currency',
    'availability',
    'snapshot_date',
    'snapshot_week',
    'scrape_timestamp',
    'run_id',
    'upload_batch_id',
    'bundle_slug',
    'selected_shops',
    'loaded_at',
    'source_scope',
    'source_file',
    'validation_status',
    'validation_errors',
]

VALID_UPLOAD_MODES = {'latest_only', 'full_publish'}


@dataclass(frozen=True)
class SyncEnvironment:
    database_url: str
    database_source: str
    api_url: str


@dataclass(frozen=True)
class BundleInfo:
    bundle_dir: Path
    manifest: dict[str, Any]


@dataclass(frozen=True)
class PreparedUploadData:
    upload_mode: str
    preflight_dir: Path
    valid_products_path: Path
    valid_latest_path: Path
    valid_history_path: Path
    reject_products_path: Path
    reject_latest_path: Path
    reject_history_path: Path
    report_path: Path
    products_df: pd.DataFrame
    latest_df: pd.DataFrame
    history_df: pd.DataFrame
    products_rejected_df: pd.DataFrame
    latest_rejected_df: pd.DataFrame
    history_rejected_df: pd.DataFrame
    report: dict[str, Any]


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        value = value.strip().strip('"').strip("'")
        values[key.strip()] = value
    return values


def _parse_secrets_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data = tomllib.loads(path.read_text(encoding='utf-8'))
    out: dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, str):
            out[key] = value
    return out


def _load_env_values() -> tuple[dict[str, str], str]:
    candidates = [
        (ROOT_DIR / '.env.local', '.env.local'),
        (ROOT_DIR / '.env', '.env'),
        (ROOT_DIR / '.streamlit' / 'secrets.toml', '.streamlit/secrets.toml'),
    ]
    values: dict[str, str] = dict(os.environ)
    source = 'process environment'
    for path, label in candidates:
        if path.suffix == '.toml':
            file_values = _parse_secrets_file(path)
        else:
            file_values = _parse_env_file(path)
        if file_values:
            values = {**file_values, **values}
            if source == 'process environment':
                source = label
    return values, source


def resolve_sync_environment() -> SyncEnvironment:
    values, source = _load_env_values()
    database_url = (
        values.get('DATABASE_URL')
        or values.get('SUPABASE_DB_URL')
        or values.get('SUPABASE_DATABASE_URL')
        or ''
    ).strip()
    api_url = (
        values.get('SUPABASE_URL')
        or values.get('NEXT_PUBLIC_SUPABASE_URL')
        or ''
    ).strip()
    return SyncEnvironment(database_url=database_url, database_source=source, api_url=api_url)


def _shop_metadata() -> dict[str, dict[str, str]]:
    meta: dict[str, dict[str, str]] = {}
    for item in get_registry():
        key = normalize_shop(item.id)
        domain = str(item.domain or '').strip()
        country = 'NL'
        if domain.endswith('.be'):
            country = 'BE'
        elif domain.endswith('.de'):
            country = 'DE'
        meta[key] = {
            'shop': key,
            'shop_name': item.name,
            'shop_domain': domain,
            'country': country,
        }
    return meta


def list_export_bundles(limit: int = 50) -> list[BundleInfo]:
    bundles: list[BundleInfo] = []
    if not EXPORTS_DIR.exists():
        return bundles
    for manifest_path in sorted(EXPORTS_DIR.glob('*/manifest.json'), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except Exception:
            continue
        bundles.append(BundleInfo(bundle_dir=manifest_path.parent, manifest=manifest))
        if len(bundles) >= limit:
            break
    return bundles


def latest_bundle_for(selected_shops: list[str]) -> BundleInfo | None:
    selected = sorted({normalize_shop(shop) for shop in selected_shops if normalize_shop(shop)})
    for bundle in list_export_bundles(limit=100):
        shops = sorted({normalize_shop(shop) for shop in bundle.manifest.get('selected_shops', [])})
        if shops == selected:
            return bundle
    return None


def parse_selected_shops_to_bundle(selected_shops: list[str]) -> dict[str, Any]:
    return run_parser_selection_export(selected_shops)


def _report(progress: ProgressFn, message: str, log_path: Path | None = None) -> None:
    if log_path is not None:
        append_log(log_path, message)
    if progress is not None:
        progress(message)


def _table_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public' and table_name = %s
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        """
        select exists (
            select 1
            from information_schema.tables
            where table_schema = 'public' and table_name = %s
        )
        """,
        (table,),
    )
    return bool(cur.fetchone()[0])


def _copy_csv(cur, table_name: str, columns: list[str], csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    with csv_path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.reader(handle)
        row_count = max(sum(1 for _ in reader) - 1, 0)
    if row_count == 0:
        return 0
    col_sql = ', '.join(columns)
    with cur.copy(f"COPY {table_name} ({col_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy:
        with csv_path.open('r', encoding='utf-8', newline='') as handle:
            while chunk := handle.read(65536):
                copy.write(chunk)
    return row_count


def _strip_text(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, float) and pd.isna(value):
        return ''
    text = str(value).strip()
    if text.lower() in {'nan', 'none', 'null', '<na>'}:
        return ''
    return text


def _normalize_ean(value: Any) -> str:
    text = _strip_text(value)
    if not text:
        return ''
    digits = re.sub(r'\D+', '', text)
    if len(digits) in (12, 13):
        return digits
    return ''


def _normalize_decimal_text(value: Any) -> str:
    text = _strip_text(value)
    if not text:
        return ''
    cleaned = text.replace('€', '').replace('EUR', '').replace('eur', '').replace('\xa0', ' ')
    cleaned = cleaned.replace(',', '.').strip()
    cleaned = re.sub(r'[^0-9.\-]', '', cleaned)
    if cleaned.count('.') > 1:
        first = cleaned.find('.')
        cleaned = cleaned[: first + 1] + cleaned[first + 1 :].replace('.', '')
    try:
        value_dec = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return ''
    if value_dec <= 0:
        return ''
    return format(value_dec.quantize(Decimal('0.01')), 'f')


def _normalize_timestamp_text(value: Any) -> str:
    text = _strip_text(value)
    if not text:
        return ''
    normalized = text.replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        ts = pd.to_datetime(text, errors='coerce', utc=True)
        if pd.isna(ts):
            return ''
        return ts.isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _normalize_date_text(value: Any, fallback_timestamp: str = '') -> str:
    text = _strip_text(value)
    if text:
        try:
            return datetime.fromisoformat(text[:10]).date().isoformat()
        except ValueError:
            ts = pd.to_datetime(text, errors='coerce', utc=True)
            if not pd.isna(ts):
                return ts.date().isoformat()
    if fallback_timestamp:
        try:
            return datetime.fromisoformat(fallback_timestamp.replace('Z', '+00:00')).date().isoformat()
        except ValueError:
            pass
    return datetime.now(UTC).date().isoformat()


def _normalize_snapshot_week(value: Any, snapshot_date: str) -> str:
    text = _strip_text(value)
    if text:
        return text
    try:
        week_dt = datetime.fromisoformat(snapshot_date)
    except ValueError:
        week_dt = datetime.now(UTC)
    iso = week_dt.isocalendar()
    return f'{iso.year}-W{iso.week:02d}'


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = ''
    return out[columns]




def _resolve_manifest_output_path(bundle_dir: Path, raw_value: Any) -> Path:
    raw_text = _strip_text(raw_value)
    if not raw_text:
        return bundle_dir / ''
    direct = Path(raw_text)
    if direct.exists():
        return direct
    windows_name = PureWindowsPath(raw_text).name
    if windows_name:
        candidate = bundle_dir / windows_name
        if candidate.exists():
            return candidate
    candidate = bundle_dir / Path(raw_text).name
    if candidate.exists():
        return candidate
    return direct

def _read_csv_frame(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    return _ensure_columns(frame, columns)


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding='utf-8')


def _with_rejection_reason(frame: pd.DataFrame, reasons: list[str], reason_column: str = 'rejection_reason') -> pd.DataFrame:
    out = frame.copy()
    out[reason_column] = reasons
    return out


def _validate_products(products_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = products_df.copy()
    frame = _ensure_columns(frame, PRODUCT_STAGE_COLUMNS[:15])
    frame['ean_original'] = frame['ean'].map(_strip_text)
    frame['ean'] = frame['ean'].map(_normalize_ean)
    frame['artist'] = frame['artist'].map(_strip_text)
    frame['title'] = frame['title'].map(_strip_text)
    reasons = ['' for _ in range(len(frame))]
    for idx, row in enumerate(frame.itertuples(index=False)):
        row_reasons: list[str] = []
        if not row.ean:
            row_reasons.append('invalid_ean')
        if not row.artist:
            row_reasons.append('missing_artist')
        if not row.title:
            row_reasons.append('missing_title')
        reasons[idx] = '|'.join(row_reasons)
    invalid = _with_rejection_reason(frame[[col for col in frame.columns]], reasons)
    rejected = invalid[invalid['rejection_reason'] != ''].copy()
    valid = invalid[invalid['rejection_reason'] == ''].drop(columns=['rejection_reason']).copy()
    valid = valid.sort_values(['ean', 'title', 'artist'], kind='stable')
    duplicate_mask = valid.duplicated(subset=['ean'], keep='first')
    duplicate_rejects = valid[duplicate_mask].copy()
    if not duplicate_rejects.empty:
        duplicate_rejects['rejection_reason'] = 'duplicate_ean_in_bundle'
        rejected = pd.concat([rejected, duplicate_rejects], ignore_index=True)
    valid = valid[~duplicate_mask].copy()
    valid = _ensure_columns(valid.drop(columns=['ean_original'], errors='ignore'), PRODUCT_STAGE_COLUMNS[:15])
    rejected = _ensure_columns(rejected, PRODUCT_STAGE_COLUMNS[:15] + ['ean_original', 'rejection_reason'])
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def _validate_latest(latest_df: pd.DataFrame, *, selected_shops: list[str], require_shop_url: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = latest_df.copy()
    frame = _ensure_columns(frame, LATEST_STAGE_COLUMNS[:10])
    frame['ean_original'] = frame['ean'].map(_strip_text)
    frame['price_original'] = frame['current_price'].map(_strip_text)
    frame['shop'] = frame['shop'].map(normalize_shop)
    frame['ean'] = frame['ean'].map(_normalize_ean)
    frame['current_price'] = frame['current_price'].map(_normalize_decimal_text)
    frame['currency'] = frame['currency'].map(_strip_text).replace({'': 'EUR'})
    frame['shop_url'] = frame['shop_url'].map(_strip_text)
    frame['availability'] = frame['availability'].map(_normalize_availability)
    frame['last_scraped_at'] = frame['last_scraped_at'].map(_normalize_timestamp_text)
    frame['snapshot_date'] = [
        _normalize_date_text(value, fallback_timestamp=ts)
        for value, ts in zip(frame['snapshot_date'], frame['last_scraped_at'])
    ]
    frame['snapshot_week'] = [
        _normalize_snapshot_week(value, snapshot_date)
        for value, snapshot_date in zip(frame['snapshot_week'], frame['snapshot_date'])
    ]

    selected_set = {normalize_shop(shop) for shop in selected_shops if normalize_shop(shop)}
    reasons: list[str] = []
    for row in frame.itertuples(index=False):
        row_reasons: list[str] = []
        if not row.shop:
            row_reasons.append('missing_shop')
        elif selected_set and row.shop not in selected_set:
            row_reasons.append('shop_not_selected')
        if not row.ean:
            row_reasons.append('invalid_ean')
        if not row.current_price:
            row_reasons.append('invalid_price')
        if require_shop_url and not row.shop_url:
            row_reasons.append('missing_shop_url')
        if not row.last_scraped_at:
            row_reasons.append('invalid_last_scraped_at')
        reasons.append('|'.join(row_reasons))

    tagged = _with_rejection_reason(frame, reasons)
    rejected = tagged[tagged['rejection_reason'] != ''].copy()
    valid = tagged[tagged['rejection_reason'] == ''].drop(columns=['rejection_reason']).copy()
    if not valid.empty:
        valid['_sort_ts'] = pd.to_datetime(valid['last_scraped_at'], errors='coerce', utc=True)
        valid = valid.sort_values(['shop', 'ean', '_sort_ts', 'shop_url'], ascending=[True, True, False, True], kind='stable')
        duplicate_mask = valid.duplicated(subset=['shop', 'ean'], keep='first')
        duplicate_rejects = valid[duplicate_mask].copy()
        if not duplicate_rejects.empty:
            duplicate_rejects['rejection_reason'] = 'duplicate_shop_ean_in_bundle'
            rejected = pd.concat([rejected, duplicate_rejects], ignore_index=True)
        valid = valid[~duplicate_mask].drop(columns=['_sort_ts'])
    rejected = _ensure_columns(rejected, LATEST_STAGE_COLUMNS[:10] + ['ean_original', 'price_original', 'rejection_reason'])
    valid = _ensure_columns(valid, LATEST_STAGE_COLUMNS[:10])
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def _validate_history(history_df: pd.DataFrame, *, selected_shops: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = history_df.copy()
    frame = _ensure_columns(frame, HISTORY_STAGE_COLUMNS[:11])
    frame['ean_original'] = frame['ean'].map(_strip_text)
    frame['price_original'] = frame['price'].map(_strip_text)
    frame['shop'] = frame['shop'].map(normalize_shop)
    frame['ean'] = frame['ean'].map(_normalize_ean)
    frame['price'] = frame['price'].map(_normalize_decimal_text)
    frame['currency'] = frame['currency'].map(_strip_text).replace({'': 'EUR'})
    frame['shop_url'] = frame['shop_url'].map(_strip_text)
    frame['availability'] = frame['availability'].map(_normalize_availability)
    frame['scrape_timestamp'] = frame['scrape_timestamp'].map(_normalize_timestamp_text)
    frame['snapshot_date'] = [
        _normalize_date_text(value, fallback_timestamp=ts)
        for value, ts in zip(frame['snapshot_date'], frame['scrape_timestamp'])
    ]
    frame['snapshot_week'] = [
        _normalize_snapshot_week(value, snapshot_date)
        for value, snapshot_date in zip(frame['snapshot_week'], frame['snapshot_date'])
    ]

    selected_set = {normalize_shop(shop) for shop in selected_shops if normalize_shop(shop)}
    reasons: list[str] = []
    for row in frame.itertuples(index=False):
        row_reasons: list[str] = []
        if not row.shop:
            row_reasons.append('missing_shop')
        elif selected_set and row.shop not in selected_set:
            row_reasons.append('shop_not_selected')
        if not row.ean:
            row_reasons.append('invalid_ean')
        if not row.price:
            row_reasons.append('invalid_price')
        if not row.scrape_timestamp:
            row_reasons.append('invalid_scrape_timestamp')
        reasons.append('|'.join(row_reasons))

    tagged = _with_rejection_reason(frame, reasons)
    rejected = tagged[tagged['rejection_reason'] != ''].copy()
    valid = tagged[tagged['rejection_reason'] == ''].drop(columns=['rejection_reason']).copy()
    if not valid.empty:
        valid['_sort_ts'] = pd.to_datetime(valid['scrape_timestamp'], errors='coerce', utc=True)
        valid = valid.sort_values(['shop', 'ean', 'snapshot_week', '_sort_ts'], ascending=[True, True, True, False], kind='stable')
        duplicate_mask = valid.duplicated(subset=['shop', 'ean', 'snapshot_week'], keep='first')
        duplicate_rejects = valid[duplicate_mask].copy()
        if not duplicate_rejects.empty:
            duplicate_rejects['rejection_reason'] = 'duplicate_shop_ean_week_in_bundle'
            rejected = pd.concat([rejected, duplicate_rejects], ignore_index=True)
        valid = valid[~duplicate_mask].drop(columns=['_sort_ts'])
    rejected = _ensure_columns(rejected, HISTORY_STAGE_COLUMNS[:11] + ['ean_original', 'price_original', 'rejection_reason'])
    valid = _ensure_columns(valid, HISTORY_STAGE_COLUMNS[:11])
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def _prepare_upload_data(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    upload_mode: str,
    require_shop_url: bool,
) -> PreparedUploadData:
    if upload_mode not in VALID_UPLOAD_MODES:
        raise ValueError(f'Unsupported upload_mode: {upload_mode}')

    outputs = manifest.get('outputs', {})
    selected_shops = [normalize_shop(shop) for shop in manifest.get('selected_shops', []) if normalize_shop(shop)]
    products_df = _read_csv_frame(_resolve_manifest_output_path(bundle_dir, outputs['products_master']), PRODUCT_STAGE_COLUMNS[:15])
    latest_df = _read_csv_frame(_resolve_manifest_output_path(bundle_dir, outputs['offers_latest']), LATEST_STAGE_COLUMNS[:10])
    history_df = _read_csv_frame(_resolve_manifest_output_path(bundle_dir, outputs['offers_history']), HISTORY_STAGE_COLUMNS[:11])

    valid_products, rejected_products = _validate_products(products_df)
    valid_latest, rejected_latest = _validate_latest(latest_df, selected_shops=selected_shops, require_shop_url=require_shop_url)
    valid_history, rejected_history = _validate_history(history_df, selected_shops=selected_shops)

    if upload_mode == 'latest_only':
        referenced_eans = set(valid_latest['ean'].tolist())
        valid_products = valid_products[valid_products['ean'].isin(referenced_eans)].reset_index(drop=True)
        if not valid_history.empty:
            latest_only_history_rejects = valid_history.copy()
            latest_only_history_rejects['rejection_reason'] = 'skipped_in_latest_only_mode'
            rejected_history = pd.concat([rejected_history, latest_only_history_rejects], ignore_index=True)
        valid_history = pd.DataFrame(columns=HISTORY_STAGE_COLUMNS[:11])

    preflight_dir = bundle_dir / 'preflight'
    preflight_dir.mkdir(parents=True, exist_ok=True)

    valid_products_path = preflight_dir / 'products_validated.csv'
    valid_latest_path = preflight_dir / 'offers_latest_validated.csv'
    valid_history_path = preflight_dir / 'offers_history_validated.csv'
    reject_products_path = preflight_dir / 'products_rejected.csv'
    reject_latest_path = preflight_dir / 'offers_latest_rejected.csv'
    reject_history_path = preflight_dir / 'offers_history_rejected.csv'
    report_path = preflight_dir / 'preflight_report.json'

    _write_frame(valid_products_path, valid_products)
    _write_frame(valid_latest_path, valid_latest)
    _write_frame(valid_history_path, valid_history)
    _write_frame(reject_products_path, rejected_products)
    _write_frame(reject_latest_path, rejected_latest)
    _write_frame(reject_history_path, rejected_history)

    report = {
        'bundle_dir': str(bundle_dir),
        'bundle_slug': bundle_dir.name,
        'upload_mode': upload_mode,
        'selected_shops': selected_shops,
        'validated_rows': {
            'products': int(len(valid_products)),
            'offers_latest': int(len(valid_latest)),
            'offers_history': int(len(valid_history)),
        },
        'rejected_rows': {
            'products': int(len(rejected_products)),
            'offers_latest': int(len(rejected_latest)),
            'offers_history': int(len(rejected_history)),
        },
        'files': {
            'products_validated': str(valid_products_path),
            'offers_latest_validated': str(valid_latest_path),
            'offers_history_validated': str(valid_history_path),
            'products_rejected': str(reject_products_path),
            'offers_latest_rejected': str(reject_latest_path),
            'offers_history_rejected': str(reject_history_path),
        },
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    return PreparedUploadData(
        upload_mode=upload_mode,
        preflight_dir=preflight_dir,
        valid_products_path=valid_products_path,
        valid_latest_path=valid_latest_path,
        valid_history_path=valid_history_path,
        reject_products_path=reject_products_path,
        reject_latest_path=reject_latest_path,
        reject_history_path=reject_history_path,
        report_path=report_path,
        products_df=valid_products,
        latest_df=valid_latest,
        history_df=valid_history,
        products_rejected_df=rejected_products,
        latest_rejected_df=rejected_latest,
        history_rejected_df=rejected_history,
        report=report,
    )


def _create_temp_tables(cur) -> None:
    cur.execute(
        """
        create temp table if not exists temp_stage_shop_map (
            shop text,
            shop_name text,
            shop_domain text,
            country text
        );
        """
    )


def _seed_shop_map(cur) -> None:
    rows = [
        (meta['shop'], meta['shop_name'], meta['shop_domain'], meta['country'])
        for meta in _shop_metadata().values()
    ]
    cur.executemany(
        "insert into temp_stage_shop_map (shop, shop_name, shop_domain, country) values (%s, %s, %s, %s)",
        rows,
    )


def _ensure_staging_tables(cur) -> None:
    statements = [
        """
        create table if not exists public.staging_products_import (
            id bigserial primary key
        );
        """,
        """
        create table if not exists public.staging_offers_latest_import (
            id bigserial primary key
        );
        """,
        """
        create table if not exists public.staging_offers_history_import (
            id bigserial primary key
        );
        """,
    ]
    for statement in statements:
        cur.execute(statement)

    for table, columns in {
        'staging_products_import': PRODUCT_STAGE_COLUMNS,
        'staging_offers_latest_import': LATEST_STAGE_COLUMNS,
        'staging_offers_history_import': HISTORY_STAGE_COLUMNS,
    }.items():
        for column in columns:
            data_type = 'timestamptz' if column == 'loaded_at' else 'text'
            cur.execute(f"alter table public.{table} add column if not exists {column} {data_type}")
        cur.execute(f"create index if not exists idx_{table}_batch on public.{table} (upload_batch_id)")
        cur.execute(f"create index if not exists idx_{table}_loaded_at on public.{table} (loaded_at)")


def _cleanup_old_staging_rows(cur, *, keep_days: int) -> None:
    keep_days = max(int(keep_days), 1)
    for table in ['staging_products_import', 'staging_offers_latest_import', 'staging_offers_history_import']:
        cur.execute(
            f"delete from public.{table} where loaded_at is not null and loaded_at < now() - (%s || ' days')::interval",
            (keep_days,),
        )


def _sync_shops(cur) -> int:
    if not _table_exists(cur, 'shops'):
        raise RuntimeError('public.shops table not found in Supabase database.')
    cols = _table_columns(cur, 'shops')
    insert_cols: list[str] = []
    select_cols: list[str] = []
    if 'name' in cols:
        insert_cols.append('name')
        select_cols.append('sm.shop_name')
    if 'domain' in cols:
        insert_cols.append('domain')
        select_cols.append('sm.shop_domain')
    if 'country' in cols:
        insert_cols.append('country')
        select_cols.append("nullif(sm.country, '')")
    if 'is_active' in cols:
        insert_cols.append('is_active')
        select_cols.append('true')
    if not insert_cols:
        return 0
    sql = f"""
        insert into public.shops ({', '.join(insert_cols)})
        select {', '.join(select_cols)}
        from temp_stage_shop_map sm
        left join public.shops s
          on lower(coalesce(s.domain, '')) = lower(coalesce(sm.shop_domain, ''))
          or lower(coalesce(s.name, '')) = lower(coalesce(sm.shop_name, ''))
        where s.id is null
          and coalesce(sm.shop_domain, '') <> ''
    """
    cur.execute(sql)
    return cur.rowcount or 0


def _sync_products(cur, batch_id: str) -> dict[str, int]:
    if not _table_exists(cur, 'products'):
        raise RuntimeError('public.products table not found in Supabase database.')
    cols = _table_columns(cur, 'products')
    if 'ean' not in cols:
        raise RuntimeError('public.products is missing required column ean.')

    def stage_expr(column: str) -> str:
        mapping = {
            'artist': "nullif(sp.artist, '')",
            'title': "nullif(sp.title, '')",
            'format_label': "nullif(sp.format, '')",
            'cover_url': "nullif(sp.cover_art_url, '')",
            'canonical_key': "nullif(lower(trim(coalesce(sp.artist, ''))) || '::' || lower(trim(coalesce(sp.title, ''))), '::')",
            'artist_normalized': "nullif(lower(trim(coalesce(sp.artist, ''))), '')",
            'title_normalized': "nullif(lower(trim(coalesce(sp.title, ''))), '')",
            'search_text': "nullif(trim(concat_ws(' ', sp.artist, sp.title, sp.ean)), '')",
        }
        return mapping[column]

    update_sets = []
    for column in ['artist', 'title', 'format_label', 'cover_url', 'canonical_key', 'artist_normalized', 'title_normalized', 'search_text']:
        if column in cols:
            update_sets.append(f"{column} = coalesce({stage_expr(column)}, p.{column})")
    if 'updated_at' in cols:
        update_sets.append('updated_at = now()')
    if update_sets:
        cur.execute(
            f"""
            update public.products p
               set {', '.join(update_sets)}
              from public.staging_products_import sp
             where p.ean = sp.ean
               and sp.upload_batch_id = %s
               and char_length(coalesce(sp.ean, '')) in (12, 13)
            """,
            (batch_id,),
        )
        updated = cur.rowcount or 0
    else:
        updated = 0

    insert_cols = ['ean']
    insert_select = ['sp.ean']
    for column in ['artist', 'title', 'format_label', 'cover_url', 'canonical_key', 'artist_normalized', 'title_normalized', 'search_text']:
        if column in cols:
            insert_cols.append(column)
            insert_select.append(stage_expr(column))
    cur.execute(
        f"""
        insert into public.products ({', '.join(insert_cols)})
        select {', '.join(insert_select)}
          from public.staging_products_import sp
          left join public.products p on p.ean = sp.ean
         where sp.upload_batch_id = %s
           and p.id is null
           and char_length(coalesce(sp.ean, '')) in (12, 13)
        """,
        (batch_id,),
    )
    inserted = cur.rowcount or 0
    return {'inserted': inserted, 'updated': updated}


def _resolved_latest_cte(price_cols: set[str]) -> str:
    product_url_select = "nullif(ol.shop_url, '') as product_url"
    if 'product_url' not in price_cols and 'url' in price_cols:
        product_url_select = "nullif(ol.shop_url, '') as url"
    return f"""
        with src as (
            select distinct on (ol.ean, sm.shop_domain)
                p.id as product_id,
                s.id as shop_id,
                cast(nullif(ol.current_price, '') as numeric) as price,
                nullif(ol.currency, '') as currency,
                {product_url_select},
                coalesce(nullif(ol.availability, ''), 'unknown') as availability,
                coalesce(nullif(ol.last_scraped_at, '')::timestamptz, now()) as scraped_at
              from public.staging_offers_latest_import ol
              join temp_stage_shop_map sm on sm.shop = ol.shop
              join public.products p on p.ean = ol.ean
              join public.shops s
                on lower(coalesce(s.domain, '')) = lower(coalesce(sm.shop_domain, ''))
                or lower(coalesce(s.name, '')) = lower(coalesce(sm.shop_name, ''))
             where ol.upload_batch_id = %s
               and char_length(coalesce(ol.ean, '')) in (12, 13)
               and nullif(ol.current_price, '') is not null
             order by ol.ean, sm.shop_domain, coalesce(nullif(ol.last_scraped_at, '')::timestamptz, now()) desc
        )
    """


def _sync_prices(cur, batch_id: str) -> dict[str, int]:
    if not _table_exists(cur, 'prices'):
        raise RuntimeError('public.prices table not found in Supabase database.')
    cols = _table_columns(cur, 'prices')
    for required in ['product_id', 'shop_id', 'price']:
        if required not in cols:
            raise RuntimeError(f'public.prices is missing required column {required}.')

    url_col = 'product_url' if 'product_url' in cols else ('url' if 'url' in cols else None)
    update_sets = ['price = src.price']
    if 'currency' in cols:
        update_sets.append("currency = coalesce(src.currency, prices.currency)")
    if url_col:
        update_sets.append(f"{url_col} = coalesce(src.{url_col}, prices.{url_col})")
    if 'availability' in cols:
        update_sets.append("availability = coalesce(src.availability, prices.availability)")
    if 'first_seen_at' in cols:
        update_sets.append('first_seen_at = least(coalesce(prices.first_seen_at, src.scraped_at), src.scraped_at)')
    if 'last_seen_at' in cols:
        update_sets.append('last_seen_at = greatest(coalesce(prices.last_seen_at, src.scraped_at), src.scraped_at, coalesce(prices.first_seen_at, src.scraped_at))')
    if 'is_active' in cols:
        update_sets.append('is_active = true')
    if 'updated_at' in cols:
        update_sets.append('updated_at = now()')
    cur.execute(
        _resolved_latest_cte(cols)
        + f"""
        update public.prices
           set {', '.join(update_sets)}
          from src
         where prices.product_id = src.product_id
           and prices.shop_id = src.shop_id
        """,
        (batch_id,),
    )
    updated = cur.rowcount or 0

    insert_cols = ['product_id', 'shop_id', 'price']
    select_cols = ['src.product_id', 'src.shop_id', 'src.price']
    if 'currency' in cols:
        insert_cols.append('currency')
        select_cols.append("coalesce(src.currency, 'EUR')")
    if url_col:
        insert_cols.append(url_col)
        select_cols.append(f'src.{url_col}')
    if 'availability' in cols:
        insert_cols.append('availability')
        select_cols.append("coalesce(src.availability, 'unknown')")
    if 'first_seen_at' in cols:
        insert_cols.append('first_seen_at')
        select_cols.append('src.scraped_at')
    if 'last_seen_at' in cols:
        insert_cols.append('last_seen_at')
        select_cols.append('src.scraped_at')
    if 'is_active' in cols:
        insert_cols.append('is_active')
        select_cols.append('true')
    cur.execute(
        _resolved_latest_cte(cols)
        + f"""
        insert into public.prices ({', '.join(insert_cols)})
        select {', '.join(select_cols)}
          from src
          left join public.prices prices
            on prices.product_id = src.product_id
           and prices.shop_id = src.shop_id
         where prices.id is null
        """,
        (batch_id,),
    )
    inserted = cur.rowcount or 0
    return {'inserted': inserted, 'updated': updated}


def _history_timestamp_expr(history_cols: set[str]) -> tuple[str | None, str | None]:
    for candidate in ['checked_at', 'observed_at', 'captured_at', 'created_at']:
        if candidate in history_cols:
            return candidate, "coalesce(nullif(oh.scrape_timestamp, '')::timestamptz, nullif(oh.snapshot_date, '')::date::timestamptz, now())"
    for candidate in ['observed_on', 'snapshot_date', 'day']:
        if candidate in history_cols:
            return candidate, "coalesce(nullif(oh.snapshot_date, '')::date, current_date)"
    return None, None


def _sync_price_history(cur, batch_id: str) -> int:
    if not _table_exists(cur, 'price_history'):
        return 0
    cols = _table_columns(cur, 'price_history')
    for required in ['product_id', 'shop_id', 'price']:
        if required not in cols:
            return 0
    time_col, time_expr = _history_timestamp_expr(cols)
    url_col = 'product_url' if 'product_url' in cols else ('url' if 'url' in cols else None)
    insert_cols = ['product_id', 'shop_id', 'price']
    select_cols = ['p.id', 's.id', "cast(nullif(oh.price, '') as numeric)"]
    if 'currency' in cols:
        insert_cols.append('currency')
        select_cols.append("coalesce(nullif(oh.currency, ''), 'EUR')")
    if url_col:
        insert_cols.append(url_col)
        select_cols.append("nullif(oh.shop_url, '')")
    if 'availability' in cols:
        insert_cols.append('availability')
        select_cols.append("coalesce(nullif(oh.availability, ''), 'unknown')")
    if 'snapshot_week' in cols:
        insert_cols.append('snapshot_week')
        select_cols.append("nullif(oh.snapshot_week, '')")
    if time_col and time_expr:
        insert_cols.append(time_col)
        select_cols.append(time_expr)

    dedupe_conditions = [
        'existing.product_id = p.id',
        'existing.shop_id = s.id',
        "existing.price = cast(nullif(oh.price, '') as numeric)",
    ]
    if time_col and time_expr:
        dedupe_conditions.append(f'existing.{time_col} = {time_expr}')

    cur.execute(
        f"""
        insert into public.price_history ({', '.join(insert_cols)})
        select {', '.join(select_cols)}
          from public.staging_offers_history_import oh
          join temp_stage_shop_map sm on sm.shop = oh.shop
          join public.products p on p.ean = oh.ean
          join public.shops s
            on lower(coalesce(s.domain, '')) = lower(coalesce(sm.shop_domain, ''))
            or lower(coalesce(s.name, '')) = lower(coalesce(sm.shop_name, ''))
         where oh.upload_batch_id = %s
           and char_length(coalesce(oh.ean, '')) in (12, 13)
           and nullif(oh.price, '') is not null
           and not exists (
                select 1
                  from public.price_history existing
                 where {' and '.join(dedupe_conditions)}
           )
        """,
        (batch_id,),
    )
    return cur.rowcount or 0


def _build_stage_frame(
    source_df: pd.DataFrame,
    *,
    columns: list[str],
    batch_id: str,
    bundle_slug: str,
    selected_shops: list[str],
    source_scope: str,
    source_file: str,
) -> pd.DataFrame:
    frame = _ensure_columns(source_df.copy(), columns[: len(columns) - 8])
    frame['upload_batch_id'] = batch_id
    frame['bundle_slug'] = bundle_slug
    frame['selected_shops'] = ','.join(sorted({normalize_shop(shop) for shop in selected_shops if normalize_shop(shop)}))
    frame['loaded_at'] = datetime.now(UTC).isoformat()
    frame['source_scope'] = source_scope
    frame['source_file'] = source_file
    frame['validation_status'] = 'valid'
    frame['validation_errors'] = ''
    return _ensure_columns(frame, columns)


def _shop_subset_frames(prepared: PreparedUploadData, shop: str, batch_id: str, bundle_slug: str, selected_shops: list[str]) -> dict[str, pd.DataFrame]:
    latest = prepared.latest_df[prepared.latest_df['shop'].map(normalize_shop) == shop].copy()
    history = prepared.history_df[prepared.history_df['shop'].map(normalize_shop) == shop].copy()
    referenced_eans = set(latest['ean'].tolist()) | set(history['ean'].tolist())
    products = prepared.products_df[prepared.products_df['ean'].isin(referenced_eans)].copy() if referenced_eans else prepared.products_df.iloc[0:0].copy()
    return {
        'products': _build_stage_frame(
            products,
            columns=PRODUCT_STAGE_COLUMNS,
            batch_id=batch_id,
            bundle_slug=bundle_slug,
            selected_shops=selected_shops,
            source_scope=prepared.upload_mode,
            source_file='products_validated.csv',
        ),
        'offers_latest': _build_stage_frame(
            latest,
            columns=LATEST_STAGE_COLUMNS,
            batch_id=batch_id,
            bundle_slug=bundle_slug,
            selected_shops=selected_shops,
            source_scope=prepared.upload_mode,
            source_file='offers_latest_validated.csv',
        ),
        'offers_history': _build_stage_frame(
            history,
            columns=HISTORY_STAGE_COLUMNS,
            batch_id=batch_id,
            bundle_slug=bundle_slug,
            selected_shops=selected_shops,
            source_scope=prepared.upload_mode,
            source_file='offers_history_validated.csv',
        ),
    }


def _write_stage_batch_csvs(preflight_dir: Path, shop: str, frames: dict[str, pd.DataFrame]) -> dict[str, Path]:
    batch_dir = preflight_dir / 'shop_batches' / shop
    batch_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        'products': batch_dir / 'products_stage.csv',
        'offers_latest': batch_dir / 'offers_latest_stage.csv',
        'offers_history': batch_dir / 'offers_history_stage.csv',
    }
    _write_frame(paths['products'], frames['products'])
    _write_frame(paths['offers_latest'], frames['offers_latest'])
    _write_frame(paths['offers_history'], frames['offers_history'])
    return paths


def _load_batch_into_staging(cur, batch_csvs: dict[str, Path]) -> dict[str, int]:
    return {
        'products': _copy_csv(cur, 'public.staging_products_import', PRODUCT_STAGE_COLUMNS, batch_csvs['products']),
        'offers_latest': _copy_csv(cur, 'public.staging_offers_latest_import', LATEST_STAGE_COLUMNS, batch_csvs['offers_latest']),
        'offers_history': _copy_csv(cur, 'public.staging_offers_history_import', HISTORY_STAGE_COLUMNS, batch_csvs['offers_history']),
    }


def push_bundle_to_supabase(
    bundle_dir: Path,
    *,
    progress: ProgressFn = None,
    upload_mode: str = 'full_publish',
    require_shop_url: bool = True,
    keep_staging_days: int = 30,
) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError(f'psycopg is not installed: {PSYCOPG_IMPORT_ERROR}')
    if upload_mode not in VALID_UPLOAD_MODES:
        raise ValueError(f'Unsupported upload_mode: {upload_mode}')

    env = resolve_sync_environment()
    if not env.database_url:
        raise RuntimeError('DATABASE_URL ontbreekt. Zet de Supabase/Postgres connectiestring in .env.local, .env of .streamlit/secrets.toml.')

    manifest_path = bundle_dir / 'manifest.json'
    if not manifest_path.exists():
        raise FileNotFoundError(f'No manifest.json found in export bundle: {bundle_dir}')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))

    prepared = _prepare_upload_data(bundle_dir, manifest, upload_mode=upload_mode, require_shop_url=require_shop_url)
    selected_shops = prepared.report.get('selected_shops', [])
    selected_shops = [normalize_shop(shop) for shop in selected_shops if normalize_shop(shop)]
    if not selected_shops:
        selected_shops = sorted({normalize_shop(shop) for shop in prepared.latest_df['shop'].tolist() if normalize_shop(shop)})

    log_path = LOGS_DIR / f"supabase_bulk_upload_{timestamp_slug()}.log"
    run_id = create_run(
        area='sync',
        target_id='supabase',
        run_type=f'bulk_staging_merge_{upload_mode}',
        status='running',
        log_path=str(log_path),
        details={
            'bundle_dir': str(bundle_dir),
            'selected_shops': selected_shops,
            'upload_mode': upload_mode,
            'preflight_report': prepared.report,
        },
    )
    _report(progress, f'Run #{run_id} bulk upload started.', log_path)
    _report(progress, f"Preflight: products valid={prepared.report['validated_rows']['products']} rejected={prepared.report['rejected_rows']['products']} | latest valid={prepared.report['validated_rows']['offers_latest']} rejected={prepared.report['rejected_rows']['offers_latest']} | history valid={prepared.report['validated_rows']['offers_history']} rejected={prepared.report['rejected_rows']['offers_history']}", log_path)

    if prepared.latest_df.empty and (upload_mode == 'latest_only' or prepared.history_df.empty):
        raise RuntimeError('No valid rows left after preflight validation. Check the preflight report and rejected CSV files in the export bundle.')

    try:
        overall_staged = {'products': 0, 'offers_latest': 0, 'offers_history': 0}
        overall_changes = {
            'shops_inserted': 0,
            'products_inserted': 0,
            'products_updated': 0,
            'prices_inserted': 0,
            'prices_updated': 0,
            'price_history_inserted': 0,
        }
        per_shop_results: list[dict[str, Any]] = []

        with psycopg.connect(
            env.database_url,
            options='-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0',
        ) as conn:
            with conn.cursor() as cur:
                _create_temp_tables(cur)
                _seed_shop_map(cur)
                _ensure_staging_tables(cur)
                _cleanup_old_staging_rows(cur, keep_days=keep_staging_days)
                conn.commit()

            for shop in selected_shops:
                shop_batch_id = f"{bundle_dir.name}:{shop}:{uuid.uuid4().hex[:8]}"
                frames = _shop_subset_frames(prepared, shop, shop_batch_id, bundle_dir.name, selected_shops)
                if frames['offers_latest'].empty and frames['offers_history'].empty:
                    _report(progress, f'[{shop}] skipped: no valid rows after preflight for this shop.', log_path)
                    per_shop_results.append({'shop': shop, 'status': 'skipped', 'reason': 'no_valid_rows'})
                    continue
                batch_csvs = _write_stage_batch_csvs(prepared.preflight_dir, shop, frames)
                with conn.cursor() as cur:
                    staged_rows = _load_batch_into_staging(cur, batch_csvs)
                    inserted_shops = _sync_shops(cur)
                    product_stats = _sync_products(cur, shop_batch_id)
                    price_stats = _sync_prices(cur, shop_batch_id)
                    history_inserted = _sync_price_history(cur, shop_batch_id) if upload_mode == 'full_publish' else 0
                    conn.commit()

                overall_staged['products'] += staged_rows['products']
                overall_staged['offers_latest'] += staged_rows['offers_latest']
                overall_staged['offers_history'] += staged_rows['offers_history']
                overall_changes['shops_inserted'] += inserted_shops
                overall_changes['products_inserted'] += product_stats['inserted']
                overall_changes['products_updated'] += product_stats['updated']
                overall_changes['prices_inserted'] += price_stats['inserted']
                overall_changes['prices_updated'] += price_stats['updated']
                overall_changes['price_history_inserted'] += history_inserted
                shop_result = {
                    'shop': shop,
                    'upload_batch_id': shop_batch_id,
                    'staged_rows': staged_rows,
                    'db_changes': {
                        'shops_inserted': inserted_shops,
                        'products_inserted': product_stats['inserted'],
                        'products_updated': product_stats['updated'],
                        'prices_inserted': price_stats['inserted'],
                        'prices_updated': price_stats['updated'],
                        'price_history_inserted': history_inserted,
                    },
                }
                per_shop_results.append(shop_result)
                _report(
                    progress,
                    f"[{shop}] staged products={staged_rows['products']} latest={staged_rows['offers_latest']} history={staged_rows['offers_history']} | products +{product_stats['inserted']}/upd {product_stats['updated']} | prices +{price_stats['inserted']}/upd {price_stats['updated']} | history +{history_inserted}",
                    log_path,
                )

        result = {
            'run_id': run_id,
            'bundle_dir': str(bundle_dir),
            'selected_shops': selected_shops,
            'upload_mode': upload_mode,
            'preflight': prepared.report,
            'staged_rows': overall_staged,
            'db_changes': overall_changes,
            'per_shop_results': per_shop_results,
            'database_source': env.database_source,
            'database_url_present': True,
        }
        update_run(
            run_id,
            status='success',
            records_in=overall_staged['products'] + overall_staged['offers_latest'] + overall_staged['offers_history'],
            records_out=sum(overall_changes.values()),
            details=result,
        )
        _report(progress, 'Bulk upload finished successfully.', log_path)
        return result
    except Exception as exc:
        update_run(
            run_id,
            status='failed',
            details={
                'error': str(exc),
                'bundle_dir': str(bundle_dir),
                'selected_shops': selected_shops,
                'upload_mode': upload_mode,
                'preflight_report': prepared.report,
            },
        )
        _report(progress, f'Bulk upload failed: {type(exc).__name__}: {exc}', log_path)
        raise


def parse_and_push_selected_shops(
    selected_shops: list[str],
    *,
    progress: ProgressFn = None,
    upload_mode: str = 'full_publish',
    require_shop_url: bool = True,
    keep_staging_days: int = 30,
) -> dict[str, Any]:
    manifest = parse_selected_shops_to_bundle(selected_shops)
    result = push_bundle_to_supabase(
        Path(manifest['bundle_dir']),
        progress=progress,
        upload_mode=upload_mode,
        require_shop_url=require_shop_url,
        keep_staging_days=keep_staging_days,
    )
    result['parse_manifest'] = manifest
    return result
