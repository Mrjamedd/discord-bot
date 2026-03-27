from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from assets import CANONICAL_ASSET_DIR, build_script_products, validate_script_asset_directory


class AssetCatalogTests(unittest.TestCase):
    def test_build_script_products_uses_canonical_asset_directory(self) -> None:
        products = build_script_products()

        self.assertEqual(CANONICAL_ASSET_DIR, products[0].file_path.parent)
        self.assertEqual(
            (
                "Corex-Aim_2K26.gpc",
                "GOLDEN_FREE_v2.gpc",
                "secretofscript(unrealeased) (1).gpc",
                "SWOOSH V2.gpc",
            ),
            tuple(product.file_path.name for product in products),
        )
        self.assertEqual(
            (
                "CoreX Aim 2K26",
                "Golden Free Aim V2",
                "Secret of Scripts V6",
                "Swish V2",
            ),
            tuple(product.label for product in products),
        )

    def test_validate_script_asset_directory_reports_missing_directory(self) -> None:
        missing_dir = REPO_ROOT / "tests" / "missing-assets"
        products = build_script_products(asset_dir=missing_dir)

        errors = validate_script_asset_directory(products, asset_dir=missing_dir)

        self.assertEqual(1, len(errors))
        self.assertIn("missing", errors[0].lower())
        self.assertIn(str(missing_dir), errors[0])

    def test_validate_script_asset_directory_reports_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir)
            products = build_script_products(asset_dir=asset_dir)

            errors = validate_script_asset_directory(products, asset_dir=asset_dir)

        self.assertEqual(1, len(errors))
        self.assertIn("empty", errors[0].lower())
        self.assertIn(str(asset_dir), errors[0])

    def test_validate_script_asset_directory_reports_missing_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir)
            products = build_script_products(asset_dir=asset_dir)
            for product in products[:-1]:
                product.file_path.write_text("placeholder", encoding="utf-8")

            errors = validate_script_asset_directory(products, asset_dir=asset_dir)

        self.assertEqual(1, len(errors))
        self.assertIn("Swish V2", errors[0])
        self.assertIn("SWOOSH V2.gpc", errors[0])


if __name__ == "__main__":
    unittest.main()
