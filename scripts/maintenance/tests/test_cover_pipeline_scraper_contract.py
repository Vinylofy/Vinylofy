from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

WRITER_FILES = (
    "scripts/scrapers/usf/core/listing_price_sync.py",
    "scripts/scrapers/usf/jobs/promote_cdhal.py",
    "scripts/scrapers/usf/jobs/promote_fiftiesstore.py",
    "scripts/scrapers/usf/jobs/promote_myrecordstore.py",
    "scripts/scrapers/usf/jobs/promote_northendhaarlem.py",
    "scripts/scrapers/usf/jobs/promote_shop3345.py",
    "scripts/scrapers/usf/jobs/promote_sounds.py",
    "scripts/scrapers/usf/jobs/promote_soundsvenlo.py",
    "scripts/scrapers/usf/jobs/promote_staged_offers.py",
    "scripts/scrapers/usf/jobs/promote_variaworld.py",
)


class ScraperCoverContractTests(unittest.TestCase):
    def test_shop_images_are_candidate_only(self) -> None:
        checked_contracts = 0

        for filename in WRITER_FILES:
            path = ROOT / filename
            tree = ast.parse(
                path.read_text(encoding="utf-8"),
                filename=str(path),
            )

            file_contracts = 0

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue

                keywords = {
                    kw.arg: kw.value
                    for kw in node.keywords
                    if kw.arg
                }

                candidate = keywords.get("cover_candidate_url")

                if not (
                    isinstance(candidate, ast.Name)
                    and candidate.id == "image_url"
                ):
                    continue

                cover = keywords.get("cover_url")

                self.assertIsInstance(
                    cover,
                    ast.Constant,
                    msg=f"{filename}: cover_url moet expliciet None zijn",
                )
                self.assertIsNone(
                    cover.value,
                    msg=f"{filename}: shopimage mag geen publicatiecover zijn",
                )

                file_contracts += 1
                checked_contracts += 1

            self.assertEqual(
                file_contracts,
                1,
                msg=f"{filename}: verwacht exact één kandidaat-only record",
            )

        self.assertEqual(checked_contracts, 10)


if __name__ == "__main__":
    unittest.main()
