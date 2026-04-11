from __future__ import annotations

import asyncio
import json
import logging
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from admin_email import AdminEmailNotifier, AdminEmailSettings
from bot import (
    DiscordPurchaseBot,
    EMAIL_TEST_BODY,
    EMAIL_TEST_SUBJECT,
    WEEKLY_SALES_REPORT_SUBJECT,
)
from purchase_logger import PaymentMethodSummary, PurchaseLogger, SalesSummary


class AdminEmailNotifierTests(unittest.TestCase):
    @patch("admin_email.smtplib.SMTP")
    def test_send_email_uses_tls_login_and_message_payload(self, smtp_class: MagicMock) -> None:
        smtp_client = smtp_class.return_value.__enter__.return_value
        notifier = AdminEmailNotifier(
            logging.getLogger("test_admin_email_notifier"),
            settings=AdminEmailSettings(
                recipients=("mr6jam3@gmail.com",),
                sender_address="scriptz292@gmail.com",
                smtp_host="smtp.gmail.com",
                smtp_port=587,
                smtp_password="secret",
                timeout_seconds=30,
            ),
        )

        sent_ok = notifier.send_email(
            subject="Test Subject",
            body="Test Body",
            notification_type="unit_test",
        )

        self.assertTrue(sent_ok)
        smtp_client.starttls.assert_called_once()
        smtp_client.login.assert_called_once_with(
            "scriptz292@gmail.com",
            "secret",
        )
        smtp_client.send_message.assert_called_once()
        sent_message = smtp_client.send_message.call_args.args[0]
        self.assertEqual("Test Subject", sent_message["Subject"])
        self.assertEqual("scriptz292@gmail.com", sent_message["From"])
        self.assertEqual("mr6jam3@gmail.com", sent_message["To"])


class PurchaseLoggerSalesSummaryTests(unittest.TestCase):
    def test_summarize_sales_aggregates_revenue_and_payment_methods(self) -> None:
        purchase_logger = PurchaseLogger(logging.getLogger("test_purchase_logger_sales"))
        with tempfile.TemporaryDirectory() as temp_dir:
            purchase_log_path = Path(temp_dir) / "purchase_log.jsonl"
            recovery_path = Path(temp_dir) / "purchase_sync_recovery.jsonl"
            purchase_logger.purchase_log_file = purchase_log_path
            purchase_logger.recovery_file = recovery_path
            purchase_log_path.write_text(
                "\n".join(
                    (
                        json.dumps(
                            {
                                "Exact Timestamp": "2026-04-07T14:00:00+00:00",
                                "Price Paid": "20.00",
                                "Payment Method": "Cash App",
                                "Purchase Event ID": "purchase-1",
                            }
                        ),
                        json.dumps(
                            {
                                "Exact Timestamp": "2026-04-08T15:30:00+00:00",
                                "Price Paid": "$15.00",
                                "Payment Method": "PayPal",
                                "Purchase Event ID": "purchase-2",
                            }
                        ),
                        json.dumps(
                            {
                                "Exact Timestamp": "2026-04-08T16:30:00+00:00",
                                "Price Paid": "10.00",
                                "Purchase Event ID": "purchase-3",
                            }
                        ),
                        json.dumps(
                            {
                                "Exact Timestamp": "2026-03-20T10:00:00+00:00",
                                "Price Paid": "99.00",
                                "Payment Method": "Cash App",
                                "Purchase Event ID": "purchase-old",
                            }
                        ),
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            recovery_path.write_text(
                json.dumps(
                    {
                        "journal_event": "pending",
                        "purchase_event_id": "purchase-4",
                        "logged_at": "2026-04-08T17:00:00+00:00",
                        "record": {
                            "Exact Timestamp": "2026-04-08T17:00:00+00:00",
                            "Price Paid": "12.00",
                            "Payment Method": "Cash App",
                            "Purchase Event ID": "purchase-4",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            summary = purchase_logger.summarize_sales(
                period_start_utc=datetime(2026, 4, 7, tzinfo=timezone.utc),
                period_end_utc=datetime(2026, 4, 14, tzinfo=timezone.utc),
            )

        self.assertEqual(4, summary.total_sales_count)
        self.assertEqual(Decimal("57.00"), summary.total_revenue)
        breakdown = {
            item.label: (item.sales_count, item.revenue)
            for item in summary.payment_method_breakdown
        }
        self.assertEqual((2, Decimal("32.00")), breakdown["Cash App"])
        self.assertEqual((1, Decimal("15.00")), breakdown["PayPal"])
        self.assertEqual((1, Decimal("10.00")), breakdown["Unknown"])


class DiscordPurchaseBotEmailTests(unittest.IsolatedAsyncioTestCase):
    def test_build_payment_parser_failure_message_handles_timeout(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)

        message = DiscordPurchaseBot.build_payment_parser_failure_message(
            bot,
            {"reason": "payment parser timed out"},
        )

        self.assertIn("taking longer than expected", message)
        self.assertIn("Check My Payment", message)

    async def test_handle_email_test_command_requires_administrator_permissions(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)
        bot.logger = logging.getLogger("test_email_command_permissions")
        bot.send_response = AsyncMock()

        message = SimpleNamespace(
            content="!EMAILTEST",
            guild=object(),
            author=SimpleNamespace(
                id=123,
                guild_permissions=SimpleNamespace(administrator=False),
            ),
            channel=SimpleNamespace(id=456),
        )

        handled = await DiscordPurchaseBot.handle_email_test_command(bot, message)

        self.assertTrue(handled)
        bot.send_response.assert_awaited_once_with(
            message,
            "You need Administrator permissions to use `!EMAILTEST`.",
        )

    async def test_handle_email_test_command_sends_email_for_administrator(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)
        bot.logger = logging.getLogger("test_email_command_success")
        bot.send_response = AsyncMock()
        bot.send_admin_notification_email = AsyncMock(return_value=True)

        message = SimpleNamespace(
            content="!emailtest",
            guild=object(),
            author=SimpleNamespace(
                id=123,
                guild_permissions=SimpleNamespace(administrator=True),
            ),
            channel=SimpleNamespace(id=456),
        )

        handled = await DiscordPurchaseBot.handle_email_test_command(bot, message)

        self.assertTrue(handled)
        bot.send_admin_notification_email.assert_awaited_once_with(
            subject=EMAIL_TEST_SUBJECT,
            body=EMAIL_TEST_BODY,
            notification_type="email_test",
        )
        bot.send_response.assert_awaited_once_with(
            message,
            "Email system test sent to the admin recipient.",
        )

    async def test_handle_support_ticket_button_sends_support_email_alert(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)
        bot.logger = logging.getLogger("test_support_ticket_email")
        bot.interaction_custom_id = lambda interaction: "dc_bot:open_support_ticket"
        bot.audit_purchase_event = AsyncMock()
        bot.get_support_ticket_category = AsyncMock(
            return_value=SimpleNamespace(guild=SimpleNamespace(id=999))
        )
        bot.support_ticket_creation_lock = asyncio.Lock()
        bot.find_existing_ticket_channel = lambda category, user_id: None
        ticket_channel = MagicMock(spec=discord.TextChannel)
        ticket_channel.id = 222
        ticket_channel.name = "support-ticket-tester"
        ticket_channel.mention = "#support-ticket-tester"
        bot.create_support_ticket_channel = AsyncMock(return_value=ticket_channel)
        bot.send_support_ticket_alert = AsyncMock(return_value=True)

        member = SimpleNamespace(id=111, name="tester", display_name="Tester")
        interaction = SimpleNamespace(
            guild=SimpleNamespace(id=999, get_member=lambda user_id: member),
            user=SimpleNamespace(id=111, name="tester", display_name="Tester"),
            response=SimpleNamespace(send_message=AsyncMock()),
        )

        await DiscordPurchaseBot.handle_support_ticket_button(bot, interaction)

        interaction.response.send_message.assert_awaited_once_with(
            "Your support ticket is ready: #support-ticket-tester",
            ephemeral=True,
        )
        bot.send_support_ticket_alert.assert_awaited_once()
        alert_args = bot.send_support_ticket_alert.await_args.args
        self.assertIs(ticket_channel, alert_args[0])
        self.assertEqual(member, alert_args[1])

    async def test_send_weekly_sales_report_sends_expected_summary_email(self) -> None:
        bot = object.__new__(DiscordPurchaseBot)
        bot.logger = logging.getLogger("test_weekly_sales_report")
        bot.purchase_logger = MagicMock()
        bot.purchase_logger.summarize_sales = MagicMock(
            return_value=SalesSummary(
                period_start_utc=datetime(2026, 4, 3, 9, 0, tzinfo=timezone.utc),
                period_end_utc=datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
                total_sales_count=2,
                total_revenue=Decimal("35.00"),
                payment_method_breakdown=(
                    PaymentMethodSummary(
                        label="Cash App",
                        sales_count=1,
                        revenue=Decimal("20.00"),
                    ),
                    PaymentMethodSummary(
                        label="PayPal",
                        sales_count=1,
                        revenue=Decimal("15.00"),
                    ),
                ),
            )
        )
        bot.send_admin_notification_email = AsyncMock(return_value=True)

        sent_ok = await DiscordPurchaseBot.send_weekly_sales_report(
            bot,
            report_end_utc=datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(sent_ok)
        bot.send_admin_notification_email.assert_awaited_once()
        self.assertEqual(
            WEEKLY_SALES_REPORT_SUBJECT,
            bot.send_admin_notification_email.await_args.kwargs["subject"],
        )
        body = bot.send_admin_notification_email.await_args.kwargs["body"]
        self.assertIn("Total Sales Count: 2", body)
        self.assertIn("Total Revenue: $35.00", body)
        self.assertIn("Cash App: 1 sale(s), $20.00", body)
        self.assertIn("PayPal: 1 sale(s), $15.00", body)


if __name__ == "__main__":
    unittest.main()
