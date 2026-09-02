from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopDefinition:
    key: str
    scraper_module: str
    importer_module: str
    output_dir: str
    enabled: bool = True
    notes: str = ""


STABLE_SHOPS: dict[str, ShopDefinition] = {
    "blackvinyl": ShopDefinition(
        key="blackvinyl",
        scraper_module="scripts.scrapers.blackvinyl",
        importer_module="scripts.importers.import_blackvinyl",
        output_dir="data/raw/blackvinyl",
        notes="WooCommerce Store API LP Nieuw discovery with full pagination and detail EAN fallback.",
    ),
    "bobsvinyl": ShopDefinition(
        key="bobsvinyl",
        scraper_module="scripts.scrapers.bobsvinyl",
        importer_module="scripts.importers.import_bobsvinyl",
        output_dir="data/raw/bobsvinyl",
        notes="Stable enough for USF orchestration.",
    ),
    "dgmoutlet": ShopDefinition(
        key="dgmoutlet",
        scraper_module="scripts.scrapers.dgmoutlet",
        importer_module="scripts.importers.import_dgmoutlet",
        output_dir="data/raw/dgmoutlet",
        notes="Stable enough for USF orchestration.",
    ),
    "platenzaak": ShopDefinition(
        key="platenzaak",
        scraper_module="scripts.scrapers.platenzaak",
        importer_module="scripts.importers.import_platenzaak",
        output_dir="data/raw/platenzaak",
        notes="Stable enough for USF orchestration.",
    ),
    "platomania": ShopDefinition(
        key="platomania",
        scraper_module="scripts.scrapers.platomania",
        importer_module="scripts.importers.import_platomania",
        output_dir="data/raw/platomania",
        notes="Wrapper around legacy scraper; stable enough for orchestration.",
    ),
    "recordsonvinyl": ShopDefinition(
        key="recordsonvinyl",
        scraper_module="scripts.scrapers.recordsonvinyl",
        importer_module="scripts.importers.import_recordsonvinyl",
        output_dir="data/raw/recordsonvinyl",
        notes="Stable but rate-limit sensitive; use conservative limits.",
    ),
    "soundshaarlem": ShopDefinition(
        key="soundshaarlem",
        scraper_module="scripts.scrapers.soundshaarlem",
        importer_module="scripts.importers.import_soundshaarlem",
        output_dir="data/raw/soundshaarlem",
        notes="Stable enough for USF orchestration.",
    ),
    "soundsvenlo": ShopDefinition(
        key="soundsvenlo",
        scraper_module="scripts.scrapers.soundsvenlo",
        importer_module="scripts.importers.import_soundsvenlo",
        output_dir="data/raw/soundsvenlo",
        notes="Stable enough for USF orchestration.",
    ),
    "variaworld": ShopDefinition(
        key="variaworld",
        scraper_module="scripts.scrapers.variaworld",
        importer_module="scripts.importers.import_variaworld",
        output_dir="data/raw/variaworld",
        notes="Stable enough for USF orchestration.",
    ),
    "imusic": ShopDefinition(
        key="imusic",
        scraper_module="scripts.scrapers.usf.jobs.detail_imusic",
        importer_module="scripts.scrapers.usf.jobs.promote_imusic",
        output_dir="data/raw/imusic",
        notes="EAN-only USF detail lookup; no catalog discovery.",
    ),
    "jpc": ShopDefinition(
        key="jpc",
        scraper_module="scripts.scrapers.usf.jobs.detail_jpc",
        importer_module="scripts.scrapers.usf.jobs.promote_jpc",
        output_dir="data/raw/jpc",
        notes="Large-catalog JPC vinyl discovery plus batched EAN detail enrichment.",
    ),

}


EXCLUDED_SHOPS: dict[str, str] = {
    "shop3345": "Explicitly excluded: not stable enough yet.",
    "hhv": "Explicitly excluded: not stable enough yet.",
    "music_on_vinyl": "Explicitly excluded: not stable enough yet.",
}


def get_shop(key: str) -> ShopDefinition:
    try:
        return STABLE_SHOPS[key]
    except KeyError as exc:
        excluded_reason = EXCLUDED_SHOPS.get(key)
        if excluded_reason:
            raise KeyError(f"Shop {key!r} is excluded from USF v1: {excluded_reason}") from exc
        raise KeyError(f"Unknown USF shop: {key!r}") from exc


def list_enabled_shops() -> list[str]:
    return [key for key, shop in STABLE_SHOPS.items() if shop.enabled]
