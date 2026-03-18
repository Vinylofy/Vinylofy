from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
PYTHON_BIN = sys.executable

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.contracts import ShopImporterDefinition  # noqa: E402
from scripts.importers.registry import get_shop_importer, iter_shop_importers  # noqa: E402


@dataclass(frozen=True)
class ShopPipelineConfig:
    key: str
    shop_name: str
    shop_domain: str
    scraper_command_env: str
    csv_output_path: str
    importer_module: str
    rejects_path: str
    summary_path: str
    storage_prefix: str

    @classmethod
    def from_importer_definition(cls, definition: ShopImporterDefinition) -> "ShopPipelineConfig":
        return cls(
            key=definition.key,
            shop_name=definition.shop_name,
            shop_domain=definition.shop_domain,
            scraper_command_env=definition.scraper_command_env,
            csv_output_path=definition.files.csv_output_path,
            importer_module=definition.importer_module,
            rejects_path=definition.files.rejects_path,
            summary_path=definition.files.summary_path,
            storage_prefix=definition.storage_prefix,
        )

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
    definition.key: ShopPipelineConfig.from_importer_definition(definition)
    for definition in iter_shop_importers()
}


def get_shop_config(key: str) -> ShopPipelineConfig:
    definition = get_shop_importer(key)
    return SHOPS[definition.key]
