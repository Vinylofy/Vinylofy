from __future__ import annotations

from scripts.importers.contracts import ShopImporterDefinition
from scripts.importers.import_bobsvinyl import SHOP_DEFINITION as BOBSVINYL_IMPORTER
from scripts.importers.import_dgmoutlet import SHOP_DEFINITION as DGMOUTLET_IMPORTER
from scripts.importers.import_platomania import SHOP_DEFINITION as PLATOMANIA_IMPORTER

SHOP_IMPORTERS: dict[str, ShopImporterDefinition] = {
    definition.key: definition
    for definition in (
        BOBSVINYL_IMPORTER,
        DGMOUTLET_IMPORTER,
        PLATOMANIA_IMPORTER,
    )
}


def list_shop_keys() -> list[str]:
    return sorted(SHOP_IMPORTERS)


def iter_shop_importers() -> list[ShopImporterDefinition]:
    return [SHOP_IMPORTERS[key] for key in list_shop_keys()]


def get_shop_importer(key: str) -> ShopImporterDefinition:
    normalized = key.strip().lower()
    if normalized not in SHOP_IMPORTERS:
        available = ", ".join(list_shop_keys())
        raise KeyError(f"Unknown importer '{key}'. Available: {available}")
    return SHOP_IMPORTERS[normalized]
