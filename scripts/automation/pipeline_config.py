from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
PYTHON_BIN = sys.executable


@dataclass(frozen=True)
class ShopPipelineConfig:
    key: str
    shop_name: str
    scraper_command_env: str
    csv_output_path: str
    importer_module: str
    rejects_path: str
    summary_path: str
    storage_prefix: str

    @property
    def importer_command(self) -> list[str]:
        return [
            PYTHON_BIN,
            "-m",
            self.importer_module,
            str(PROJECT_ROOT / self.csv_output_path),
            "--rejects",
            str(PROJECT_ROOT / self.rejects_path),
            "--summary",
            str(PROJECT_ROOT / self.summary_path),
        ]


SHOPS: dict[str, ShopPipelineConfig] = {
    "bobsvinyl": ShopPipelineConfig(
        key="bobsvinyl",
        shop_name="Bob's Vinyl",
        scraper_command_env="VINYLOFY_SCRAPER_CMD_BOBSVINYL",
        csv_output_path="data/raw/bobsvinyl/bobsvinyl_step2_enriched.csv",
        importer_module="scripts.importers.import_bobsvinyl",
        rejects_path="output/bobsvinyl_rejects.csv",
        summary_path="output/bobsvinyl_import_summary.json",
        storage_prefix="bobsvinyl",
    ),
    "dgmoutlet": ShopPipelineConfig(
        key="dgmoutlet",
        shop_name="DGM Outlet",
        scraper_command_env="VINYLOFY_SCRAPER_CMD_DGMOUTLET",
        csv_output_path="data/raw/dgmoutlet/dgmoutlet_products.csv",
        importer_module="scripts.importers.import_dgmoutlet",
        rejects_path="output/dgmoutlet_rejects.csv",
        summary_path="output/dgmoutlet_import_summary.json",
        storage_prefix="dgmoutlet",
    ),
    "platomania": ShopPipelineConfig(
        key="platomania",
        shop_name="Platomania",
        scraper_command_env="VINYLOFY_SCRAPER_CMD_PLATOMANIA",
        csv_output_path="data/raw/platomania/platomania_step2_enriched.csv",
        importer_module="scripts.importers.import_platomania",
        rejects_path="output/platomania_rejects.csv",
        summary_path="output/platomania_import_summary.json",
        storage_prefix="platomania",
    ),
}


def get_shop_config(key: str) -> ShopPipelineConfig:
    normalized = key.strip().lower()
    if normalized not in SHOPS:
        available = ", ".join(sorted(SHOPS))
        raise KeyError(f"Unknown shop '{key}'. Available: {available}")
    return SHOPS[normalized]