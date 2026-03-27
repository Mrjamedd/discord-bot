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
    def test_build_script_products_falls_back_to_known_catalog_for_missing_canonical_dir(self) -> None:
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
                "Golden V2",
                "Secret of Scripts V6",
                "Swish V2",
            ),
            tuple(product.label for product in products),
        )
        self.assertIn("golden free v2", products[1].aliases)
        self.assertIn("golden v2", products[1].aliases)

    def test_build_script_products_uses_live_gpc_files_from_asset_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir)
            (asset_dir / "custom_script.gpc").write_text("custom", encoding="utf-8")
            (asset_dir / "GOLDEN_FREE_v2.gpc").write_text("golden", encoding="utf-8")
            (asset_dir / "zeta-test.gpc").write_text("zeta", encoding="utf-8")
            (asset_dir / "ignore-me.txt").write_text("ignore", encoding="utf-8")

            products = build_script_products(asset_dir=asset_dir)

        self.assertEqual(
            ("custom_script.gpc", "GOLDEN_FREE_v2.gpc", "zeta-test.gpc"),
            tuple(product.file_path.name for product in products),
        )
        self.assertEqual(
            ("Custom Script", "Golden V2", "Zeta Test"),
            tuple(product.label for product in products),
        )
        self.assertEqual(("1", "2", "3"), tuple(product.aliases[0] for product in products))

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

    def test_validate_script_asset_directory_reports_missing_gpc_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir)
            (asset_dir / "notes.txt").write_text("placeholder", encoding="utf-8")

            errors = validate_script_asset_directory((), asset_dir=asset_dir)

        self.assertEqual(1, len(errors))
        self.assertIn(".gpc", errors[0].lower())
        self.assertIn(str(asset_dir), errors[0])

    def test_validate_script_asset_directory_accepts_live_dynamic_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir)
            (asset_dir / "custom_script.gpc").write_text("placeholder", encoding="utf-8")
            (asset_dir / "GOLDEN_FREE_v2.gpc").write_text("placeholder", encoding="utf-8")
            products = build_script_products(asset_dir=asset_dir)

            errors = validate_script_asset_directory(products, asset_dir=asset_dir)

        self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()
