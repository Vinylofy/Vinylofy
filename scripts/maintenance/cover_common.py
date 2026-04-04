#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import io
import json
import mimetypes
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Package 'psycopg' is required for the cover pipeline.") from exc

def require_pillow():
    try:
        from PIL import Image, UnidentifiedImageError  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Package 'Pillow' is required for image preparation in the cover pipeline.") from exc
    return Image, UnidentifiedImageError


SHOP_PRIORITY_DEFAULTS: dict[str, int] = {
    "platomania.nl": 100,
    "recordsonvinyl.nl": 95,
    "platenzaak.nl": 92,
    "bobsvinyl.nl": 90,
    "variaworld.nl": 85,
    "groovespin.com": 80,
    "soundsvenlo.nl": 78,
    "soundshaarlem.nl": 76,
    "dgmoutlet.nl": 74,
    "3345.nl": 70,
}

SOURCE_TYPE_SCORES: dict[str, int] = {
    "listing": 45,
    "detail": 35,
    "jsonld": 30,
    "og": 28,
    "twitter": 26,
    "image_src": 24,
    "meta": 20,
    "img_tag": 12,
    "unknown": 0,
}

CANDIDATE_STATUSES = {"pending", "accepted", "rejected", "failed", "published"}
QUEUE_STATUSES = {"pending", "processing", "published", "failed", "review", "retry_later"}

DEFAULT_CONNECT_TIMEOUT = 15
DEFAULT_READ_TIMEOUT = 30
DEFAULT_MAX_IMAGE_DIMENSION = 1200
DEFAULT_MIN_IMAGE_DIMENSION = 250
DEFAULT_IMAGE_QUALITY = 88
DEFAULT_MAX_OFFERS_PER_PRODUCT = 3
DEFAULT_RETRY_AFTER_HOURS = 12


@dataclass(slots=True)
class OfferSource:
    product_id: str
    ean: str
    shop_id: str | None
    shop_domain: str
    shop_name: str | None
    product_url: str
    cover_priority: int
    offer_rank: int
    last_seen_at: datetime | None


@dataclass(slots=True)
class CandidateRecord:
    product_id: str
    ean: str
    shop_id: str | None
    shop_domain: str
    shop_name: str | None
    product_url: str
    image_url: str
    source_type: str
    source_rank: int
    is_primary: bool = False
    mime_type: str | None = None
    width: int | None = None
    height: int | None = None


@dataclass(slots=True)
class PreparedImage:
    output_bytes: bytes
    extension: str
    mime_type: str
    width: int
    height: int
    sha256: str
    original_mime_type: str | None
    public_url: str | None = None


class CoverPipelineError(RuntimeError):
    pass


def log(message: str) -> None:
    print(message, flush=True)


def utc_now() -> datetime:
    return datetime.now(UTC)


def load_env() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)


def get_database_url() -> str:
    load_env()
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise CoverPipelineError("DATABASE_URL ontbreekt.")
    return db_url


def get_supabase_credentials() -> tuple[str, str, str, str]:
    load_env()
    url = os.getenv("SUPABASE_URL", "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SECRET_KEY") or "").strip()
    bucket = os.getenv("VINYLOFY_COVER_STORAGE_BUCKET", "").strip()
    prefix = os.getenv("VINYLOFY_COVER_STORAGE_PREFIX", "covers/products").strip().strip("/")
    if not url or not key or not bucket:
        raise CoverPipelineError(
            "SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SECRET_KEY en VINYLOFY_COVER_STORAGE_BUCKET zijn verplicht."
        )
    return url.rstrip("/"), key, bucket, prefix


def connect_db():
    return psycopg.connect(get_database_url())


def ensure_runtime_directories() -> Path:
    root = Path("output") / "cover_pipeline"
    root.mkdir(parents=True, exist_ok=True)
    return root


def make_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    value = str(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def normalize_ean(value: str | None) -> str | None:
    digits = re.sub(r"\D", "", value or "")
    if not digits:
        return None
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) not in (8, 12, 13, 14):
        return None
    return digits


def normalize_candidate_url(base_url: str, candidate_url: str | None) -> str | None:
    raw = normalize_text(candidate_url)
    if not raw:
        return None
    if raw.startswith("data:"):
        return None
    if raw.startswith("//"):
        parts = urlsplit(base_url)
        return f"{parts.scheme}:{raw}"
    absolute = urljoin(base_url, raw)
    parts = urlsplit(absolute)
    cleaned = urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
    return cleaned


def get_table_columns(conn, table_name: str, schema: str = "public") -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            order by ordinal_position
            """,
            (schema, table_name),
        )
        return {str(row[0]) for row in cur.fetchall()}


def require_table_columns(conn, table_name: str, required: Iterable[str], schema: str = "public") -> set[str]:
    columns = get_table_columns(conn, table_name, schema=schema)
    missing = [column for column in required if column not in columns]
    if missing:
        raise CoverPipelineError(
            f"Tabel {schema}.{table_name} mist verplichte kolommen: {', '.join(missing)}. Draai eerst de cover migration."
        )
    return columns


def quote_public_storage_path(path: str) -> str:
    return "/".join(quote(part, safe="") for part in path.split("/"))


def build_public_storage_url(supabase_url: str, bucket: str, remote_path: str) -> str:
    return f"{supabase_url}/storage/v1/object/public/{bucket}/{quote_public_storage_path(remote_path)}"


def load_shop_priority_map() -> dict[str, int]:
    raw = os.getenv("VINYLOFY_COVER_SHOP_PRIORITY_JSON", "").strip()
    if not raw:
        return dict(SHOP_PRIORITY_DEFAULTS)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CoverPipelineError("VINYLOFY_COVER_SHOP_PRIORITY_JSON bevat geen geldige JSON.") from exc
    if not isinstance(parsed, dict):
        raise CoverPipelineError("VINYLOFY_COVER_SHOP_PRIORITY_JSON moet een JSON object zijn.")
    result = dict(SHOP_PRIORITY_DEFAULTS)
    for key, value in parsed.items():
        try:
            result[str(key).lower()] = int(value)
        except (TypeError, ValueError) as exc:
            raise CoverPipelineError(
                f"VINYLOFY_COVER_SHOP_PRIORITY_JSON bevat geen geldige integer voor sleutel {key!r}."
            ) from exc
    return result


def shop_priority_for_domain(domain: str) -> int:
    return load_shop_priority_map().get(normalize_text(domain).lower(), 50)


def rank_candidate(candidate: CandidateRecord, recency_reference: datetime | None = None) -> int:
    score = 0
    score += SOURCE_TYPE_SCORES.get(candidate.source_type, 0)
    score += shop_priority_for_domain(candidate.shop_domain)
    if candidate.is_primary:
        score += 10
    if candidate.width and candidate.height:
        shortest = min(candidate.width, candidate.height)
        if shortest >= 900:
            score += 20
        elif shortest >= 600:
            score += 12
        elif shortest >= DEFAULT_MIN_IMAGE_DIMENSION:
            score += 6
        aspect = max(candidate.width, candidate.height) / max(1, shortest)
        if aspect <= 1.25:
            score += 8
        elif aspect <= 1.6:
            score += 2
        else:
            score -= 20
    if candidate.shop_id:
        score += 4
    if recency_reference is not None:
        age_hours = max(0.0, (utc_now() - recency_reference).total_seconds() / 3600)
        if age_hours <= 24:
            score += 10
        elif age_hours <= 72:
            score += 6
        elif age_hours <= 168:
            score += 2
    return score


def extract_candidates_from_json_ld(data: Any, base_url: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            image_value = node.get("image")
            if isinstance(image_value, str):
                normalized = normalize_candidate_url(base_url, image_value)
                if normalized:
                    candidates.append({"image_url": normalized, "source_type": "jsonld", "is_primary": True})
            elif isinstance(image_value, list):
                for item in image_value:
                    if isinstance(item, str):
                        normalized = normalize_candidate_url(base_url, item)
                        if normalized:
                            candidates.append(
                                {"image_url": normalized, "source_type": "jsonld", "is_primary": len(candidates) == 0}
                            )
                    elif isinstance(item, dict):
                        url_value = item.get("url") or item.get("contentUrl")
                        normalized = normalize_candidate_url(base_url, url_value)
                        if normalized:
                            candidates.append(
                                {"image_url": normalized, "source_type": "jsonld", "is_primary": len(candidates) == 0}
                            )
            for child in node.values():
                _walk(child)
        elif isinstance(node, list):
            for child in node:
                _walk(child)

    _walk(data)
    return candidates


def extract_image_candidates_from_html(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(image_url: str | None, source_type: str, *, is_primary: bool = False, width: Any = None, height: Any = None) -> None:
        normalized = normalize_candidate_url(page_url, image_url)
        if not normalized or normalized in seen:
            return
        if any(token in normalized.lower() for token in ("placeholder", "no-image", "logo", "icon")):
            return
        seen.add(normalized)
        parsed_width = int(width) if str(width).isdigit() else None
        parsed_height = int(height) if str(height).isdigit() else None
        results.append(
            {
                "image_url": normalized,
                "source_type": source_type,
                "is_primary": is_primary,
                "width": parsed_width,
                "height": parsed_height,
            }
        )

    og_image = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "og:image"})
    og_width = soup.find("meta", attrs={"property": "og:image:width"})
    og_height = soup.find("meta", attrs={"property": "og:image:height"})
    if og_image:
        add(
            og_image.get("content"),
            "og",
            is_primary=True,
            width=og_width.get("content") if og_width else None,
            height=og_height.get("content") if og_height else None,
        )

    twitter_image = soup.find("meta", attrs={"name": "twitter:image"}) or soup.find(
        "meta", attrs={"property": "twitter:image"}
    )
    if twitter_image:
        add(twitter_image.get("content"), "twitter", is_primary=not results)

    link_image = soup.find("link", attrs={"rel": lambda value: value and "image_src" in str(value)})
    if link_image:
        add(link_image.get("href"), "image_src", is_primary=not results)

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = normalize_text(script.string or script.text)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for candidate in extract_candidates_from_json_ld(parsed, page_url):
            add(candidate.get("image_url"), candidate.get("source_type", "jsonld"), is_primary=candidate.get("is_primary", False))

    img_candidates: list[tuple[int, dict[str, Any]]] = []
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-srcset")
        classes = " ".join(img.get("class", []))
        alt = normalize_text(img.get("alt"))
        hint_text = f"{classes} {alt}".lower()
        priority = 0
        if any(token in hint_text for token in ("product", "gallery", "media", "cover", "vinyl", "album", "plaat")):
            priority += 20
        if img.get("width") and img.get("height"):
            try:
                shortest = min(int(img.get("width")), int(img.get("height")))
                priority += min(shortest // 40, 20)
            except (TypeError, ValueError):
                pass
        if img.get("loading") == "lazy":
            priority += 2
        if src:
            img_candidates.append(
                (
                    priority,
                    {
                        "image_url": src,
                        "source_type": "img_tag",
                        "is_primary": False,
                        "width": img.get("width"),
                        "height": img.get("height"),
                    },
                )
            )

    for _, candidate in sorted(img_candidates, key=lambda item: item[0], reverse=True)[:5]:
        add(
            candidate.get("image_url"),
            candidate.get("source_type", "img_tag"),
            is_primary=not results,
            width=candidate.get("width"),
            height=candidate.get("height"),
        )

    return results


def fetch_page_candidates(session: requests.Session, page_url: str) -> tuple[list[dict[str, Any]], int, str | None]:
    response = session.get(page_url, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT))
    http_status = response.status_code
    response.raise_for_status()
    content_type = normalize_text(response.headers.get("content-type"))
    if "html" not in content_type and response.text.lstrip()[:1] != "<":
        raise CoverPipelineError(f"Onverwacht content-type voor productpagina: {content_type or 'onbekend'}")
    return extract_image_candidates_from_html(response.text, page_url), http_status, response.text


def fetch_binary(session: requests.Session, url: str) -> tuple[bytes, str | None]:
    response = session.get(url, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), stream=True)
    response.raise_for_status()
    content = response.content
    content_type = normalize_text(response.headers.get("content-type")) or None
    if not content:
        raise CoverPipelineError("Lege image response ontvangen.")
    return content, content_type


def prepare_image_for_storage(content: bytes, original_mime_type: str | None = None) -> PreparedImage:
    Image, UnidentifiedImageError = require_pillow()
    try:
        image = Image.open(io.BytesIO(content))
    except UnidentifiedImageError as exc:
        raise CoverPipelineError("Gedownloade binary is geen geldige afbeelding.") from exc

    image.load()
    width, height = image.size
    shortest = min(width, height)
    aspect = max(width, height) / max(1, shortest)
    if shortest < DEFAULT_MIN_IMAGE_DIMENSION:
        raise CoverPipelineError(
            f"Afbeelding te klein ({width}x{height}); minimum is {DEFAULT_MIN_IMAGE_DIMENSION}px aan de kortste zijde."
        )
    if aspect > 1.8:
        raise CoverPipelineError(f"Afbeelding heeft onwaarschijnlijke aspect ratio voor cover-art ({width}x{height}).")

    if image.mode not in ("RGB", "RGBA"):
        image = image.convert("RGBA" if "A" in image.getbands() else "RGB")

    if max(width, height) > DEFAULT_MAX_IMAGE_DIMENSION:
        image.thumbnail((DEFAULT_MAX_IMAGE_DIMENSION, DEFAULT_MAX_IMAGE_DIMENSION))
        width, height = image.size

    if image.mode == "RGBA":
        background = Image.new("RGB", image.size, (255, 255, 255))
        background.paste(image, mask=image.getchannel("A"))
        image = background
    elif image.mode != "RGB":
        image = image.convert("RGB")

    buffer = io.BytesIO()
    image.save(buffer, format="WEBP", quality=DEFAULT_IMAGE_QUALITY, method=6)
    output_bytes = buffer.getvalue()
    sha256 = hashlib.sha256(output_bytes).hexdigest()

    return PreparedImage(
        output_bytes=output_bytes,
        extension="webp",
        mime_type="image/webp",
        width=width,
        height=height,
        sha256=sha256,
        original_mime_type=original_mime_type,
    )


def build_storage_path(prefix: str, ean: str, sha256_hash: str, extension: str) -> str:
    ean_clean = normalize_ean(ean)
    if not ean_clean:
        raise CoverPipelineError(f"Kan geen storage pad bouwen zonder geldige EAN: {ean!r}")
    return f"{prefix}/ean/{ean_clean[:3]}/{ean_clean}/{sha256_hash[:16]}.{extension}"


def upload_bytes_to_storage(remote_path: str, prepared_image: PreparedImage) -> str:
    supabase_url, key, bucket, _ = get_supabase_credentials()
    try:
        from supabase import create_client
    except ImportError as exc:  # pragma: no cover
        raise CoverPipelineError("Package 'supabase' ontbreekt.") from exc

    client = create_client(supabase_url, key)
    payload = io.BytesIO(prepared_image.output_bytes)
    payload.seek(0)
    client.storage.from_(bucket).upload(
        path=remote_path,
        file=payload,
        file_options={
            "content-type": prepared_image.mime_type,
            "upsert": "true",
        },
    )
    return build_public_storage_url(supabase_url, bucket, remote_path)


def build_cover_missing_condition(products_columns: set[str], alias: str = "p") -> str:
    clauses: list[str] = []
    for column in ("cover_storage_path", "cover_url"):
        if column in products_columns:
            clauses.append(f"coalesce(nullif({alias}.{column}, ''), '') = ''")
    if not clauses:
        return "true"
    return " and ".join(clauses)


def build_cover_priority_expression(products_columns: set[str], alias: str = "p") -> str:
    if "cover_priority" in products_columns:
        return f"coalesce({alias}.cover_priority, 0)"
    return "0"


def safe_parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    raw = normalize_text(value)
    if not raw:
        return None
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def serialize_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def next_retry_timestamp(hours: int = DEFAULT_RETRY_AFTER_HOURS) -> datetime:
    return utc_now() + timedelta(hours=hours)
