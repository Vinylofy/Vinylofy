from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable

from scripts.importers.common import CanonicalRecord, ImportConfig


RowMapper = Callable[[dict, int], tuple[CanonicalRecord | None, str | None]]
BeforeRunHook = Callable[[str], None]


@dataclass(frozen=True)
class ImportFileLayout:
    csv_output_path: str
    rejects_path: str
    summary_path: str


@dataclass(frozen=True)
class ShopImporterDefinition:
    key: str
    config: ImportConfig
    importer_module: str
    scraper_command_env: str
    storage_prefix: str
    files: ImportFileLayout
    row_mapper: RowMapper
    description: str = ""
    required_columns: tuple[str, ...] = field(default_factory=tuple)
    optional_columns: tuple[str, ...] = field(default_factory=tuple)
    tags: tuple[str, ...] = field(default_factory=tuple)
    before_run: BeforeRunHook | None = None

    @property
    def shop_name(self) -> str:
        return self.config.shop_name

    @property
    def shop_domain(self) -> str:
        return self.config.shop_domain

    @property
    def shop_country(self) -> str:
        return self.config.shop_country

    @property
    def currency(self) -> str:
        return self.config.currency

    @property
    def all_declared_columns(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for column in [*self.required_columns, *self.optional_columns]:
            if column not in ordered:
                ordered.append(column)
        return tuple(ordered)


@dataclass(frozen=True)
class SourceValidationResult:
    csv_path: str
    headers: tuple[str, ...]
    missing_required_columns: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.missing_required_columns


@dataclass(frozen=True)
class OnboardingChecklist:
    shop_key: str
    checklist_items: tuple[str, ...]


DEFAULT_ONBOARDING_CHECKLIST = OnboardingChecklist(
    shop_key="template",
    checklist_items=(
        "Scraper schrijft stabiele CSV met vaste kolomnamen weg",
        "Row mapper zet bronvelden om naar CanonicalRecord",
        "required_columns dekken alle verplichte bronvelden af",
        "Importer module staat geregistreerd in scripts.importers.registry",
        "Pipeline config wordt automatisch uit registry afgeleid",
        "Lokale dry-run en pipeline dry-run zijn gevalideerd",
    ),
)
