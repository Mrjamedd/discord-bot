from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from models import ScriptProduct
from purchase_audit_logger import PURCHASE_AUDIT_COLUMNS, _build_purchase_audit_row
from ticketing import (
    PURCHASE_TICKET_AUTO_CLOSE_MINUTES,
    build_ticket_change_script_message,
    build_ticket_catalog_lines,
    build_ticket_retry_message,
    build_ticket_store_message,
    message_requests_script_change,
    message_requests_ticket_close,
    resolve_script_product_selection,
)


class PurchaseAuditLoggingTests(unittest.TestCase):
    def test_build_purchase_audit_row_uses_stable_schema(self) -> None:
        row = _build_purchase_audit_row(
            {
                "logged_at_utc": "2026-03-27T12:34:56+00:00",
                "event_type": "product_selection_resolved",
                "event_category": "selection",
                "status": "success",
                "trigger": "user_message",
                "ticket_stage": "awaiting_confirmation",
                "discord_user_id": 123,
                "discord_username": "tester",
                "channel_id": 456,
                "selected_product_key": "golden-free-aim-v2",
                "selected_product_filename": "GOLDEN_FREE_v2.gpc",
                "details": {"candidate_keys": ["golden-free-aim-v2"]},
            }
        )

        self.assertEqual(len(PURCHASE_AUDIT_COLUMNS), len(row))
        self.assertEqual("product_selection_resolved", row[2])
        self.assertEqual("selection", row[3])
        self.assertEqual("success", row[4])
        self.assertEqual("awaiting_confirmation", row[6])
        self.assertEqual("123", row[9])
        self.assertEqual("golden-free-aim-v2", row[23])
        self.assertEqual("GOLDEN_FREE_v2.gpc", row[25])
        self.assertIn("candidate_keys", row[-1])

    def test_ticket_catalog_lines_include_delivery_filenames(self) -> None:
        catalog_lines = build_ticket_catalog_lines()

        self.assertIn("CoreX Aim 2K26", catalog_lines)
        self.assertIn("Corex-Aim_2K26.gpc", catalog_lines)
        self.assertIn("Golden V2", catalog_lines)
        self.assertIn("delivery file:", catalog_lines)

    def test_ticket_retry_message_can_include_confirmation_hint(self) -> None:
        retry_message = build_ticket_retry_message(include_confirmation_hint=True)

        self.assertIn("Type yes to confirm and proceed", retry_message)
        self.assertIn("delivery filename", retry_message)

    def test_ticket_store_message_includes_change_and_close_note(self) -> None:
        ticket_store_message = build_ticket_store_message("tester")

        self.assertIn("`change script`", ticket_store_message)
        self.assertIn("`close ticket`", ticket_store_message)
        self.assertIn(
            f"{PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes",
            ticket_store_message,
        )

    def test_ticket_change_script_message_repeats_management_note(self) -> None:
        change_script_message = build_ticket_change_script_message()

        self.assertIn(
            "Your current script selection has been cleared",
            change_script_message,
        )
        self.assertIn("`change script`", change_script_message)
        self.assertIn("`close ticket`", change_script_message)

    def test_ticket_management_commands_are_normalized(self) -> None:
        self.assertTrue(message_requests_script_change("Change Script"))
        self.assertTrue(message_requests_ticket_close("CLOSE TICKET"))

    def test_resolve_script_product_selection_reports_match(self) -> None:
        result = resolve_script_product_selection("golden free aim")

        self.assertEqual("matched", result.status)
        self.assertIsNotNone(result.product)
        self.assertEqual("golden-free-aim-v2", result.product.key)

    def test_resolve_script_product_selection_supports_renamed_golden_label(self) -> None:
        result = resolve_script_product_selection("golden v2")

        self.assertEqual("matched", result.status)
        self.assertIsNotNone(result.product)
        self.assertEqual("Golden V2", result.product.label)

    def test_resolve_script_product_selection_reports_unmatched(self) -> None:
        result = resolve_script_product_selection("not a real product")

        self.assertEqual("unmatched", result.status)
        self.assertIsNone(result.product)

    def test_resolve_script_product_selection_reports_ambiguous(self) -> None:
        shared_alias_products = (
            ScriptProduct(
                key="first-product",
                label="First Product",
                price=23,
                file_path=Path("/tmp/first.gpc"),
                aliases=("shared alias",),
            ),
            ScriptProduct(
                key="second-product",
                label="Second Product",
                price=23,
                file_path=Path("/tmp/second.gpc"),
                aliases=("shared alias",),
            ),
        )

        with patch("ticketing.SCRIPT_PRODUCTS", shared_alias_products):
            result = resolve_script_product_selection("shared alias")

        self.assertEqual("ambiguous", result.status)
        self.assertIsNone(result.product)
        self.assertGreaterEqual(len(result.candidate_keys), 2)


if __name__ == "__main__":
    unittest.main()
