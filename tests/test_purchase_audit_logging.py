from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from bot import DiscordPurchaseBot
from models import ScriptProduct
from purchase_audit_logger import PURCHASE_AUDIT_COLUMNS, _build_purchase_audit_row
from ticketing import (
    PAYMENT_PLATFORMS,
    PURCHASE_TICKET_AUTO_CLOSE_MINUTES,
    build_payment_instruction_message,
    build_script_confirmation_message,
    build_ticket_change_script_message,
    build_ticket_catalog_lines,
    build_ticket_panel_message,
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

        self.assertIn("type `yes` exactly to continue", retry_message)
        self.assertIn("delivery filename", retry_message)

    def test_ticket_store_message_includes_change_and_close_note(self) -> None:
        ticket_store_message = build_ticket_store_message("tester")

        self.assertIn("`change script`", ticket_store_message)
        self.assertIn("`close ticket`", ticket_store_message)
        self.assertIn("Do this now:", ticket_store_message)
        self.assertIn(
            f"{PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes",
            ticket_store_message,
        )

    def test_ticket_panel_message_is_action_first(self) -> None:
        panel_message = build_ticket_panel_message()

        self.assertIn("Ready to buy a script?", panel_message)
        self.assertIn("`Open Purchase Ticket`", panel_message)

    def test_ticket_change_script_message_repeats_management_note(self) -> None:
        change_script_message = build_ticket_change_script_message()

        self.assertIn(
            "Your previous script and payment setup have been cleared",
            change_script_message,
        )
        self.assertIn("`change script`", change_script_message)
        self.assertIn("`close ticket`", change_script_message)

    def test_script_confirmation_message_can_show_ticket_price_override(self) -> None:
        message = build_script_confirmation_message(
            ScriptProduct(
                key="golden-free-aim-v2",
                label="Golden V2",
                price=23,
                file_path=Path("/tmp/GOLDEN_FREE_v2.gpc"),
                aliases=(),
            ),
            ticket_price_override="15.00",
        )

        self.assertIn("$15.00", message)
        self.assertIn("standard $23.00", message)

    def test_payment_instruction_message_can_show_ticket_price_override(self) -> None:
        message = build_payment_instruction_message(
            ScriptProduct(
                key="golden-free-aim-v2",
                label="Golden V2",
                price=23,
                file_path=Path("/tmp/GOLDEN_FREE_v2.gpc"),
                aliases=(),
            ),
            PAYMENT_PLATFORMS[0],
            "ZEN-AB12CD",
            ticket_price_override="12.50",
        )

        self.assertIn("$12.50", message)
        self.assertIn("standard price $23.00", message)
        self.assertIn("ZEN-AB12CD", message)
        self.assertIn("Check My Payment", message)

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


class AdminCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_delete_command_closes_purchase_ticket(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)
        bot.is_admin_bypass_user = lambda user: True
        bot.is_purchase_ticket_channel = lambda channel: True
        bot.is_support_ticket_channel = lambda channel: False
        bot.get_authoritative_ticket_owner_id = AsyncMock(return_value=123456)
        bot.get_ticket_record_snapshot = AsyncMock(
            return_value={
                "stage": "awaiting_selection",
                "selected_script_key": None,
                "payment_platform_key": None,
                "payment_note_code": None,
            }
        )
        bot.close_purchase_ticket_channel = AsyncMock(return_value=True)
        bot.audit_admin_event = AsyncMock()
        bot.audit_purchase_event = AsyncMock()
        bot.send_response = AsyncMock()

        channel = MagicMock(spec=discord.TextChannel)
        channel.id = 321
        message = SimpleNamespace(
            content="!D",
            channel=channel,
            author=SimpleNamespace(
                id=999,
                name="reports0486",
                display_name="ciga",
            ),
        )

        handled = await DiscordPurchaseBot.handle_admin_command(bot, message)

        self.assertTrue(handled)
        bot.close_purchase_ticket_channel.assert_awaited_once()
        _, kwargs = bot.close_purchase_ticket_channel.await_args
        self.assertEqual(3, kwargs["grace_period_seconds"])
        self.assertIn("deleted in a few seconds", kwargs["closing_message"])
        bot.send_response.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
