from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from scripts.importers.common import normalize_ean, strict_normalize_gtin


@dataclass(frozen=True)
class ReleaseObservation:
    source: str
    ean: str
    release_date: date
    product_id: str | None = None


@dataclass
class ReleaseDateConflict:
    ean: str
    existing_source: str
    existing_release_date: date
    incoming_source: str
    incoming_release_date: date


@dataclass
class ReleaseCandidate:
    ean: str
    gtin_normalized: str
    release_date: date
    sources: set[str] = field(default_factory=set)
    product_ids: set[str] = field(default_factory=set)
    date_conflicts: list[ReleaseDateConflict] = field(default_factory=list)


def union_release_observations(
    observations: list[ReleaseObservation],
) -> list[ReleaseCandidate]:
    candidates: dict[str, ReleaseCandidate] = {}
    first_source_by_gtin: dict[str, str] = {}

    for observation in observations:
        gtin_normalized = strict_normalize_gtin(observation.ean)
        ean = normalize_ean(observation.ean)
        if not gtin_normalized or not ean:
            continue

        candidate = candidates.get(gtin_normalized)
        if candidate is None:
            candidate = ReleaseCandidate(
                ean=ean,
                gtin_normalized=gtin_normalized,
                release_date=observation.release_date,
            )
            candidates[gtin_normalized] = candidate
            first_source_by_gtin[gtin_normalized] = observation.source

        if observation.release_date != candidate.release_date:
            candidate.date_conflicts.append(
                ReleaseDateConflict(
                    ean=ean,
                    existing_source=first_source_by_gtin[gtin_normalized],
                    existing_release_date=candidate.release_date,
                    incoming_source=observation.source,
                    incoming_release_date=observation.release_date,
                )
            )

        candidate.sources.add(observation.source)
        if observation.product_id:
            candidate.product_ids.add(observation.product_id)

    return sorted(candidates.values(), key=lambda item: (item.release_date, item.ean))
