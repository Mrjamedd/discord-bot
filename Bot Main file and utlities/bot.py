from __future__ import annotations

import asyncio
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord

from Email_Parser import check_payment_email
from admin_email import AdminEmailNotifier
from config import (
    WEEKLY_SALES_REPORT_HOUR,
    WEEKLY_SALES_REPORT_MINUTE,
    WEEKLY_SALES_REPORT_TIMEZONE,
    WEEKLY_SALES_REPORT_WEEKDAY,
    PAYMENT_BUTTON_CUSTOM_ID,
    PAYMENT_CHECK_DELAY_SECONDS,
    PAYMENT_PARSER_EXPECTED_AMOUNT,
    PAYMENT_PARSER_TIMEOUT_SECONDS,
    PURCHASE_SYNC_RETRY_INTERVAL_SECONDS,
    STATE_SAVE_RETRY_INTERVAL_SECONDS,
    SUPPORT_MODERATOR_ROLE_ID,
    SUPPORT_TICKET_BUTTON_CUSTOM_ID,
    SUPPORT_TICKET_CATEGORY_ID,
    SUPPORT_TICKET_CHANNEL_MESSAGE,
    SUPPORT_TICKET_PANEL_CHANNEL_ID,
    TICKET_CATEGORY_ID,
    TICKET_BUTTON_CUSTOM_ID,
    TICKET_PANEL_CHANNEL_ID,
)
from discord_views import (
    PaymentConfirmationView,
    PaymentPlatformSelectionView,
    SupportTicketLauncherView,
    TicketLauncherView,
    payment_platform_button_custom_id,
)
from models import (
    BotState,
    PaymentParserResult,
    PaymentPlatform,
    PurchaseRecord,
    ScriptProduct,
    TicketRecord,
)
from purchase_audit_logger import PurchaseFlowAuditLogger
from purchase_logger import PurchaseLogger, SalesSummary
from state_manager import (
    fresh_ticket_record,
    get_payment_parser_state,
    get_ticket_record,
    load_state,
    load_state_result,
    purge_consumed_message_ids,
    record_consumed_message_id,
    save_state,
)
from ticketing import (
    PAYMENT_PLATFORMS,
    PURCHASE_TICKET_AUTO_CLOSE_DELAY_SECONDS,
    PURCHASE_TICKET_AUTO_CLOSE_MINUTES,
    TICKET_STAGE_AWAITING_CONFIRMATION,
    TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
    TICKET_STAGE_AWAITING_PAYMENT,
    TICKET_STAGE_AWAITING_SELECTION,
    TICKET_STAGE_COMPLETED,
    TICKET_STAGE_PAYMENT_PENDING,
    UNSET,
    VALID_TICKET_STAGES,
    build_ticket_change_script_message,
    build_ticket_catalog_lines,
    build_payment_instruction_message,
    build_payment_platform_prompt_message,
    build_script_confirmation_message,
    build_script_delivery_file,
    build_support_ticket_channel_name,
    build_support_ticket_panel_message,
    build_ticket_channel_name,
    build_ticket_panel_message,
    build_ticket_retry_message,
    build_ticket_store_message,
    generate_payment_note_code,
    get_payment_platform_by_key,
    get_script_product_by_key,
    message_requests_script_change,
    message_requests_ticket_close,
    message_is_selection_confirmation,
    normalize_ticket_price_text,
    resolve_script_product_selection,
    resolve_ticket_price_text,
    ticket_owner_id_from_topic,
    ticket_owner_topic,
)
from utils import (
    message_has_component_custom_id,
    normalize_text,
    split_message,
    utc_timestamp,
)

ADMIN_BYPASS_USERNAME = "reports0486"
ADMIN_BYPASS_DISPLAY_NAME = "ciga"
ADMIN_COMMAND_TRIGGER = "admin_bypass"
EMAIL_TEST_COMMAND_ALIASES = frozenset({"!emailtest"})
EMAIL_TEST_SUBJECT = "IMPORTANT - Email System Test"
EMAIL_TEST_BODY = "Confirm that the mail server is successfully linked to the bot."
SUPPORT_TICKET_ALERT_SUBJECT = "URGENT - New Support Ticket Opened"
WEEKLY_SALES_REPORT_SUBJECT = "MUST READ - Weekly Sales Summary Report"
WEEKDAY_NAME_TO_NUMBER = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
ADMIN_COMMAND_LIST = "!admin help"
ADMIN_COMMAND_LIST_ALIASES = frozenset(
    {
        "!admin",
        "!admin help",
        "!admin menu",
        "!admin commands",
        "!admin command",
        "!admin list",
        "!admin test",
        "!admin test commands",
        "!admin comands",
    }
)
ADMIN_CATALOG_COMMAND_ALIASES = frozenset(
    {"!admin catalog", "!admin scripts", "!admin script catalog"}
)
ADMIN_STATUS_COMMAND_ALIASES = frozenset(
    {"!admin status", "!admin ticket", "!admin ticket status"}
)
ADMIN_VERSION_COMMAND_ALIASES = frozenset(
    {"!admin version", "!admin build", "!admin bot version"}
)
ADMIN_SET_SCRIPT_COMMAND_PREFIXES = (
    "!admin set-script",
    "!admin set script",
    "!admin script",
)
ADMIN_SET_STAGE_COMMAND_PREFIXES = (
    "!admin set-stage",
    "!admin set stage",
    "!admin stage",
)
ADMIN_SET_PRICE_COMMAND_PREFIXES = (
    "!admin set-price",
    "!admin set price",
    "!admin price",
)
ADMIN_RESET_COMMAND_ALIASES = frozenset(
    {"!admin reset-ticket", "!admin reset", "!admin reset ticket", "!admin start over"}
)
ADMIN_DELETE_TICKET_COMMAND_ALIASES = frozenset({"!d"})
ADMIN_DELIVER_COMMAND_PREFIXES = (
    "!admin deliver-file",
    "!admin deliver file",
    "!admin deliver",
)
ADMIN_BYPASS_COMMAND_PREFIXES = (
    "!admin bypass-email",
    "!admin bypass email",
    "!admin bypass",
)
BOT_REPO_ROOT = Path(__file__).resolve().parents[1]


class DiscordPurchaseBot(discord.Client):
    def __init__(
        self,
        *,
        logger: logging.Logger,
        purchase_logger: PurchaseLogger,
        audit_logger: PurchaseFlowAuditLogger,
        admin_email_notifier: AdminEmailNotifier | None = None,
    ) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.logger = logger
        self.purchase_logger = purchase_logger
        self.audit_logger = audit_logger
        self.admin_email_notifier = admin_email_notifier or AdminEmailNotifier(logger)
        self.response_allowed_mentions: discord.AllowedMentions = (
            discord.AllowedMentions.none()
        )
        self.support_ping_allowed_mentions = discord.AllowedMentions(
            everyone=False,
            users=False,
            roles=True,
            replied_user=False,
        )
        if getattr(load_state, "__module__", "") != "state_manager":
            self.state = load_state()
            self.state_load_source = "patched"
            state_load_warnings: tuple[str, ...] = ()
        else:
            state_load_result = load_state_result()
            self.state = state_load_result.state
            self.state_load_source = state_load_result.source
            state_load_warnings = state_load_result.warnings
        self.state_lock = asyncio.Lock()
        self.ticket_panel_lock = asyncio.Lock()
        self.ticket_creation_lock = asyncio.Lock()
        self.support_ticket_panel_lock = asyncio.Lock()
        self.support_ticket_creation_lock = asyncio.Lock()
        self.payment_check_tasks: set[asyncio.Task[None]] = set()
        self.pending_payment_check_channel_ids: set[int] = set()
        self.purchase_sync_lock = asyncio.Lock()
        self.purchase_sync_retry_task: asyncio.Task[None] | None = None
        self.weekly_sales_report_task: asyncio.Task[None] | None = None
        self.state_save_retry_task: asyncio.Task[None] | None = None
        self.purchase_ticket_auto_close_tasks: dict[int, asyncio.Task[None]] = {}
        self.payment_parser_lock = asyncio.Lock()
        self.state_save_failed = False
        for warning in state_load_warnings:
            self.logger.warning("state_load_warning %s", warning)

    def build_ticket_panel_view(self) -> TicketLauncherView:
        return TicketLauncherView(self)

    def build_support_ticket_panel_view(self) -> SupportTicketLauncherView:
        return SupportTicketLauncherView(self)

    def build_payment_platform_selection_view(self) -> PaymentPlatformSelectionView:
        return PaymentPlatformSelectionView(self)

    def build_payment_confirmation_view(self) -> PaymentConfirmationView:
        return PaymentConfirmationView(self)

    @staticmethod
    def format_currency(value: Decimal) -> str:
        return f"${value.quantize(Decimal('0.01'))}"

    def message_author_is_administrator(self, message: discord.Message) -> bool:
        permissions = getattr(message.author, "guild_permissions", None)
        return bool(getattr(permissions, "administrator", False))

    def weekly_report_timezone(self) -> timezone | ZoneInfo:
        try:
            return ZoneInfo(WEEKLY_SALES_REPORT_TIMEZONE)
        except ZoneInfoNotFoundError:
            self.logger.warning(
                "weekly_sales_report_timezone_invalid value=%r fallback=%s",
                WEEKLY_SALES_REPORT_TIMEZONE,
                "UTC",
            )
            return timezone.utc

    def weekly_report_weekday(self) -> int:
        resolved_weekday = WEEKDAY_NAME_TO_NUMBER.get(WEEKLY_SALES_REPORT_WEEKDAY)
        if resolved_weekday is not None:
            return resolved_weekday
        self.logger.warning(
            "weekly_sales_report_weekday_invalid value=%r fallback=%s",
            WEEKLY_SALES_REPORT_WEEKDAY,
            "monday",
        )
        return WEEKDAY_NAME_TO_NUMBER["monday"]

    def weekly_report_time(self) -> tuple[int, int]:
        hour = WEEKLY_SALES_REPORT_HOUR
        minute = WEEKLY_SALES_REPORT_MINUTE
        if not 0 <= hour <= 23:
            self.logger.warning(
                "weekly_sales_report_hour_invalid value=%s fallback=%s",
                hour,
                9,
            )
            hour = 9
        if not 0 <= minute <= 59:
            self.logger.warning(
                "weekly_sales_report_minute_invalid value=%s fallback=%s",
                minute,
                0,
            )
            minute = 0
        return hour, minute

    def next_weekly_sales_report_run_at(
        self,
        *,
        now_utc: datetime | None = None,
    ) -> datetime:
        current_utc = now_utc or datetime.now(timezone.utc)
        if current_utc.tzinfo is None:
            current_utc = current_utc.replace(tzinfo=timezone.utc)
        else:
            current_utc = current_utc.astimezone(timezone.utc)

        report_timezone = self.weekly_report_timezone()
        local_now = current_utc.astimezone(report_timezone)
        report_hour, report_minute = self.weekly_report_time()
        scheduled_local = local_now.replace(
            hour=report_hour,
            minute=report_minute,
            second=0,
            microsecond=0,
        )
        days_until_run = (self.weekly_report_weekday() - local_now.weekday()) % 7
        if days_until_run == 0 and scheduled_local <= local_now:
            days_until_run = 7
        scheduled_local += timedelta(days=days_until_run)
        return scheduled_local.astimezone(timezone.utc)

    async def send_admin_notification_email(
        self,
        *,
        subject: str,
        body: str,
        notification_type: str,
    ) -> bool:
        try:
            email_timeout_seconds = max(
                self.admin_email_notifier.settings.timeout_seconds + 5,
                10,
            )
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self.admin_email_notifier.send_email,
                    subject=subject,
                    body=body,
                    notification_type=notification_type,
                ),
                timeout=email_timeout_seconds,
            )
        except TimeoutError:
            self.logger.error(
                "admin_email_dispatch_timed_out notification_type=%s subject=%r timeout_seconds=%s timestamp=%s",
                notification_type,
                subject,
                max(self.admin_email_notifier.settings.timeout_seconds + 5, 10),
                utc_timestamp(),
            )
            return False
        except Exception:
            self.logger.exception(
                "admin_email_dispatch_failed notification_type=%s subject=%r timestamp=%s",
                notification_type,
                subject,
                utc_timestamp(),
            )
            return False

    def build_support_ticket_alert_body(
        self,
        *,
        member: discord.Member,
        ticket_channel: discord.TextChannel,
        opened_at_utc: str,
    ) -> str:
        display_name = member.display_name or member.name
        username_line = (
            f"Username: {display_name} ({member.name})"
            if display_name != member.name
            else f"Username: {member.name}"
        )
        return (
            "A new support ticket has been opened.\n\n"
            f"{username_line}\n"
            f"User ID: {member.id}\n"
            f"Ticket/Channel Name: {ticket_channel.name}\n"
            f"Timestamp: {opened_at_utc}\n"
            f"Guild: {ticket_channel.guild.name}\n"
            f"Channel ID: {ticket_channel.id}"
        )

    def build_weekly_sales_report_body(self, summary: SalesSummary) -> str:
        lines = [
            "Weekly sales summary report",
            "",
            f"Period Start (UTC): {summary.period_start_utc.isoformat()}",
            f"Period End (UTC): {summary.period_end_utc.isoformat()}",
            "",
            f"Total Sales Count: {summary.total_sales_count}",
            f"Total Revenue: {self.format_currency(summary.total_revenue)}",
            "",
            "Payment Methods Used:",
        ]
        if summary.payment_method_breakdown:
            lines.extend(
                (
                    f"- {payment_method.label}: "
                    f"{payment_method.sales_count} sale(s), "
                    f"{self.format_currency(payment_method.revenue)}"
                )
                for payment_method in summary.payment_method_breakdown
            )
        else:
            lines.append("- No sales recorded during this reporting window.")
        return "\n".join(lines)

    async def send_support_ticket_alert(
        self,
        ticket_channel: discord.TextChannel,
        member: discord.Member,
        *,
        opened_at_utc: str,
    ) -> bool:
        email_sent = await self.send_admin_notification_email(
            subject=SUPPORT_TICKET_ALERT_SUBJECT,
            body=self.build_support_ticket_alert_body(
                member=member,
                ticket_channel=ticket_channel,
                opened_at_utc=opened_at_utc,
            ),
            notification_type="support_ticket_alert",
        )
        if not email_sent:
            self.logger.warning(
                "support_ticket_alert_email_failed channel_id=%s user_id=%s timestamp=%s",
                ticket_channel.id,
                member.id,
                opened_at_utc,
            )
        return email_sent

    async def send_weekly_sales_report(
        self,
        *,
        report_end_utc: datetime | None = None,
    ) -> bool:
        normalized_end = report_end_utc or datetime.now(timezone.utc)
        if normalized_end.tzinfo is None:
            normalized_end = normalized_end.replace(tzinfo=timezone.utc)
        else:
            normalized_end = normalized_end.astimezone(timezone.utc)
        normalized_start = normalized_end - timedelta(days=7)

        try:
            sales_summary = await asyncio.to_thread(
                self.purchase_logger.summarize_sales,
                period_start_utc=normalized_start,
                period_end_utc=normalized_end,
            )
        except Exception:
            self.logger.exception(
                "weekly_sales_report_summary_failed period_start=%s period_end=%s timestamp=%s",
                normalized_start.isoformat(),
                normalized_end.isoformat(),
                utc_timestamp(),
            )
            return False

        email_sent = await self.send_admin_notification_email(
            subject=WEEKLY_SALES_REPORT_SUBJECT,
            body=self.build_weekly_sales_report_body(sales_summary),
            notification_type="weekly_sales_report",
        )
        if not email_sent:
            self.logger.warning(
                "weekly_sales_report_email_failed period_start=%s period_end=%s timestamp=%s",
                normalized_start.isoformat(),
                normalized_end.isoformat(),
                utc_timestamp(),
            )
        return email_sent

    async def weekly_sales_report_loop(self) -> None:
        try:
            while not self.is_closed():
                next_run_utc = self.next_weekly_sales_report_run_at()
                sleep_seconds = max(
                    (next_run_utc - datetime.now(timezone.utc)).total_seconds(),
                    1.0,
                )
                self.logger.info(
                    "weekly_sales_report_scheduled next_run_utc=%s sleep_seconds=%.2f",
                    next_run_utc.isoformat(),
                    sleep_seconds,
                )
                await asyncio.sleep(sleep_seconds)
                if self.is_closed():
                    return
                await self.send_weekly_sales_report(report_end_utc=next_run_utc)
        except asyncio.CancelledError:
            return
        except Exception:
            self.logger.exception(
                "weekly_sales_report_loop_failed timestamp=%s",
                utc_timestamp(),
            )

    async def handle_email_test_command(self, message: discord.Message) -> bool:
        if message.content.strip().lower() not in EMAIL_TEST_COMMAND_ALIASES:
            return False

        if message.guild is None:
            await self.send_response(
                message,
                "`!EMAILTEST` can only be used inside a server.",
            )
            return True

        if not self.message_author_is_administrator(message):
            self.logger.warning(
                "email_test_command_rejected user_id=%s channel_id=%s timestamp=%s",
                message.author.id,
                getattr(message.channel, "id", None),
                utc_timestamp(),
            )
            await self.send_response(
                message,
                "You need Administrator permissions to use `!EMAILTEST`.",
            )
            return True

        self.logger.info(
            "email_test_command_requested user_id=%s channel_id=%s timestamp=%s",
            message.author.id,
            getattr(message.channel, "id", None),
            utc_timestamp(),
        )
        email_sent = await self.send_admin_notification_email(
            subject=EMAIL_TEST_SUBJECT,
            body=EMAIL_TEST_BODY,
            notification_type="email_test",
        )
        if email_sent:
            await self.send_response(
                message,
                "Email system test sent to the admin recipient.",
            )
        else:
            await self.send_response(
                message,
                "Email system test failed. Check the bot logs for details.",
            )
        return True

    def is_admin_bypass_user(self, user: discord.abc.User) -> bool:
        display_name = getattr(user, "display_name", user.name) or user.name
        return (
            user.name == ADMIN_BYPASS_USERNAME
            and display_name == ADMIN_BYPASS_DISPLAY_NAME
        )

    def build_admin_command_panel_message(self) -> str:
        available_stages = ", ".join(sorted(VALID_TICKET_STAGES))
        return (
            "Admin test command menu\n"
            f"Access is limited to `{ADMIN_BYPASS_USERNAME}` / `{ADMIN_BYPASS_DISPLAY_NAME}`.\n"
            "All admin actions are written to Google Sheets with `admin_bypass` as the trigger.\n\n"
            "Quick help aliases:\n"
            "- `!admin`\n"
            "- `!admin help`\n"
            "- `!admin menu`\n"
            "- `!admin commands`\n\n"
            "Read-only:\n"
            "- `!admin status`: show the current ticket owner, stage, selected script, payment platform, note code, and effective ticket price\n"
            "- `!admin catalog`: show the full asset-backed script catalog\n"
            "- `!admin version`: show the deployed bot version, tag, and commit\n\n"
            "Control:\n"
            "- `!admin script <name|number|filename|alias>`: set the selected script and move the ticket to awaiting confirmation\n"
            "- `!admin price <amount>`: set a ticket-specific price override (for example `15`, `15.00`, or `$15.00`)\n"
            "- `!admin price default`: clear the ticket-specific price override and restore the standard script price\n"
            f"- `!admin stage <stage>`: force the ticket stage. Valid stages: {available_stages}\n"
            "- `!admin reset`: clear the script/payment state and return the ticket to selection\n"
            "- `!D`: delete the current purchase or support ticket channel after a short warning\n"
            "- `!admin deliver [name|number|filename|alias]`: send a file immediately without changing the ticket to completed\n"
            "- `!admin bypass [name|number|filename|alias]`: skip email verification, deliver the file, and mark the ticket completed\n\n"
            "Legacy command names still work."
        )

    def admin_command_argument(
        self,
        raw_command: str,
        lower_command: str,
        prefixes: tuple[str, ...],
    ) -> str | None:
        for prefix in prefixes:
            if lower_command == prefix:
                return ""
            prefixed_with_space = f"{prefix} "
            if lower_command.startswith(prefixed_with_space):
                return raw_command[len(prefix) :].strip()
        return None

    def run_git_version_command(self, *args: str) -> str | None:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=BOT_REPO_ROOT,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError, ValueError):
            return None

        output = completed.stdout.strip()
        return output or None

    def get_ticket_price_override(
        self,
        ticket_record: TicketRecord | None,
    ) -> str | None:
        if ticket_record is None:
            return None
        raw_override = ticket_record.get("ticket_price_override")
        if not isinstance(raw_override, str):
            return None
        return normalize_ticket_price_text(raw_override)

    def get_effective_ticket_price_text(
        self,
        product: ScriptProduct | None,
        *,
        ticket_record: TicketRecord | None = None,
    ) -> str | None:
        return resolve_ticket_price_text(
            product,
            ticket_price_override=self.get_ticket_price_override(ticket_record),
        )

    def get_effective_ticket_price_decimal(
        self,
        product: ScriptProduct | None,
        *,
        ticket_record: TicketRecord | None = None,
    ) -> Decimal | None:
        price_text = self.get_effective_ticket_price_text(
            product,
            ticket_record=ticket_record,
        )
        if price_text is None:
            return None
        return Decimal(price_text)

    def build_ticket_price_status_lines(
        self,
        product: ScriptProduct | None,
        *,
        ticket_record: TicketRecord | None = None,
    ) -> tuple[str, ...]:
        ticket_price_override = self.get_ticket_price_override(ticket_record)
        effective_price = self.get_effective_ticket_price_text(
            product,
            ticket_record=ticket_record,
        )
        if product is None:
            return (
                f"Ticket price override: ${ticket_price_override}"
                if ticket_price_override is not None
                else "Ticket price override: none",
                (
                    f"Effective price: ${effective_price} once a script is selected"
                    if effective_price is not None
                    else "Effective price: uses the selected script's standard price",
                ),
            )

        standard_price = resolve_ticket_price_text(product) or "0.00"
        return (
            f"Standard price: ${standard_price}",
            (
                f"Ticket price override: ${ticket_price_override}"
                if ticket_price_override is not None
                else "Ticket price override: none",
            ),
            f"Effective price: ${effective_price or standard_price}",
        )

    def build_admin_version_message(self) -> str:
        description = self.run_git_version_command("describe", "--tags", "--dirty", "--always")
        branch = self.run_git_version_command("rev-parse", "--abbrev-ref", "HEAD")
        commit = self.run_git_version_command("rev-parse", "--short", "HEAD")

        if description is None and branch is None and commit is None:
            return "Bot version\nVersion information is unavailable on this deployment."

        lines = ["Bot version"]
        lines.append(f"Version: {description or 'unknown'}")
        lines.append(f"Branch: {branch or 'unknown'}")
        lines.append(f"Commit: {commit or 'unknown'}")
        return "\n".join(lines)

    def build_support_ticket_manual_review_message(self, issue: str) -> str:
        return (
            f"{issue} Open a support ticket from the support panel and send your payment platform, "
            "payment address/account, payment time, and what went wrong in one message for manual review."
        )

    def build_payment_check_scheduled_message(self) -> str:
        return (
            "Payment check scheduled.\n"
            f"Do this now: wait about {PAYMENT_CHECK_DELAY_SECONDS} seconds.\n"
            "Please do not press `Check My Payment` again unless I tell you the payment was not detected.\n"
            "What happens next: I will post the result in this ticket."
        )

    def build_payment_check_running_message(self) -> str:
        return (
            "A payment check is already running for this ticket.\n"
            "Please wait for the result message before pressing `Check My Payment` again."
        )

    async def audit_admin_event(
        self,
        event_type: str,
        *,
        status: str,
        message: discord.Message,
        channel: discord.TextChannel | None = None,
        ticket_owner_id: int | None = None,
        ticket_record: TicketRecord | None = None,
        ticket_stage: str | None = None,
        previous_ticket_stage: str | None = None,
        next_ticket_stage: str | None = None,
        product: ScriptProduct | None = None,
        platform: PaymentPlatform | None = None,
        payment_note_code: str | None = None,
        failure_reason: str | None = None,
        error: BaseException | None = None,
        delivery_filename: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        await self.audit_purchase_event(
            event_type,
            event_category="admin",
            status=status,
            trigger=ADMIN_COMMAND_TRIGGER,
            channel=channel,
            message=message,
            ticket_owner_id=ticket_owner_id,
            ticket_record=ticket_record,
            ticket_stage=ticket_stage,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=next_ticket_stage,
            product=product,
            platform=platform,
            payment_note_code=payment_note_code,
            raw_user_input=message.content,
            normalized_user_input=normalize_text(message.content),
            failure_reason=failure_reason,
            error=error,
            delivery_filename=delivery_filename,
            details=details,
        )

    def interaction_custom_id(self, interaction: discord.Interaction) -> str | None:
        if isinstance(interaction.data, dict):
            custom_id = interaction.data.get("custom_id")
            if isinstance(custom_id, str):
                return custom_id
        return None

    async def resolve_user_identity(
        self,
        user_id: int,
        *,
        guild: discord.Guild | None = None,
    ) -> tuple[str, str]:
        if guild is not None:
            member = guild.get_member(user_id)
            if member is not None:
                username = member.name
                display_name = member.display_name or username
                return username, display_name

        cached_user = self.get_user(user_id)
        if cached_user is not None:
            username = cached_user.name
            display_name = getattr(cached_user, "display_name", username) or username
            return username, display_name

        try:
            fetched_user = await self.fetch_user(user_id)
        except AttributeError:
            fallback_name = f"unknown-user-{user_id}"
            return fallback_name, fallback_name
        except discord.DiscordException:
            self.logger.exception(
                "user_lookup_failed user_id=%s guild_id=%s timestamp=%s",
                user_id,
                None if guild is None else guild.id,
                utc_timestamp(),
            )
            fallback_name = f"unknown-user-{user_id}"
            return fallback_name, fallback_name

        username = fetched_user.name
        display_name = getattr(fetched_user, "display_name", username) or username
        return username, display_name

    async def audit_purchase_event(
        self,
        event_type: str,
        *,
        event_category: str,
        status: str,
        trigger: str,
        channel: discord.TextChannel | None = None,
        interaction: discord.Interaction | None = None,
        message: discord.Message | None = None,
        actor_user_id: int | None = None,
        ticket_owner_id: int | None = None,
        ticket_record: TicketRecord | None = None,
        ticket_stage: str | None = None,
        previous_ticket_stage: str | None = None,
        next_ticket_stage: str | None = None,
        product: ScriptProduct | None = None,
        platform: PaymentPlatform | None = None,
        payment_note_code: str | None = None,
        button_custom_id: str | None = None,
        raw_user_input: str | None = None,
        normalized_user_input: str | None = None,
        failure_reason: str | None = None,
        error: BaseException | None = None,
        gmail_message_id: str | None = None,
        purchase_event_id: str | None = None,
        delivery_filename: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        if not self.audit_logger.should_log():
            return

        try:
            resolved_channel = channel
            if resolved_channel is None and interaction is not None and isinstance(
                interaction.channel,
                discord.TextChannel,
            ):
                resolved_channel = interaction.channel
            if resolved_channel is None and message is not None and isinstance(
                message.channel,
                discord.TextChannel,
            ):
                resolved_channel = message.channel

            resolved_channel_guild = (
                getattr(resolved_channel, "guild", None)
                if resolved_channel is not None
                else None
            )
            guild = (
                resolved_channel_guild
                if resolved_channel is not None
                else interaction.guild
                if interaction is not None
                else message.guild
                if message is not None
                else None
            )

            actor_username = ""
            actor_display_name = ""
            if interaction is not None:
                actor_user_id = interaction.user.id
                actor_username = interaction.user.name
                actor_display_name = (
                    getattr(interaction.user, "display_name", interaction.user.name)
                    or interaction.user.name
                )
            elif message is not None:
                actor_user_id = message.author.id
                actor_username = message.author.name
                actor_display_name = (
                    getattr(message.author, "display_name", message.author.name)
                    or message.author.name
                )

            if actor_user_id is not None and not actor_username:
                actor_username, actor_display_name = await self.resolve_user_identity(
                    actor_user_id,
                    guild=guild,
                )

            if ticket_record is not None and ticket_owner_id is None:
                owner_id_value = ticket_record.get("owner_id")
                if isinstance(owner_id_value, int) and not isinstance(owner_id_value, bool):
                    ticket_owner_id = owner_id_value

            ticket_owner_username = ""
            if ticket_owner_id is not None:
                if actor_user_id == ticket_owner_id and actor_username:
                    ticket_owner_username = actor_username
                else:
                    ticket_owner_username, _ = await self.resolve_user_identity(
                        ticket_owner_id,
                        guild=guild,
                    )

            if ticket_record is not None and ticket_stage is None:
                stage_value = ticket_record.get("stage")
                if isinstance(stage_value, str):
                    ticket_stage = stage_value

            if product is None and ticket_record is not None:
                product = get_script_product_by_key(
                    cast(str | None, ticket_record.get("selected_script_key"))
                )
            if platform is None and ticket_record is not None:
                platform = get_payment_platform_by_key(
                    cast(str | None, ticket_record.get("payment_platform_key"))
                )
            if payment_note_code is None and ticket_record is not None:
                payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
            if delivery_filename is None and product is not None:
                delivery_filename = product.file_path.name
            if button_custom_id is None and interaction is not None:
                button_custom_id = self.interaction_custom_id(interaction)
            details_payload = dict(details or {})
            ticket_price_override = self.get_ticket_price_override(ticket_record)
            if ticket_price_override is not None:
                details_payload.setdefault("ticket_price_override", ticket_price_override)
            if trigger == ADMIN_COMMAND_TRIGGER:
                details_payload.setdefault("admin_bypass", True)
                details_payload.setdefault("processed_via", "admin_bypass")

            event: dict[str, object] = {
                "logged_at_utc": utc_timestamp(),
                "event_type": event_type,
                "event_category": event_category,
                "status": status,
                "trigger": trigger,
                "ticket_stage": ticket_stage or next_ticket_stage or previous_ticket_stage or "",
                "previous_ticket_stage": previous_ticket_stage or "",
                "next_ticket_stage": next_ticket_stage or "",
                "discord_user_id": actor_user_id,
                "discord_username": actor_username,
                "discord_display_name": actor_display_name,
                "ticket_owner_id": ticket_owner_id,
                "ticket_owner_username": ticket_owner_username,
                "channel_id": None if resolved_channel is None else getattr(resolved_channel, "id", None),
                "channel_name": None if resolved_channel is None else getattr(resolved_channel, "name", ""),
                "guild_id": None if guild is None else guild.id,
                "guild_name": None if guild is None else guild.name,
                "message_id": (
                    message.id
                    if message is not None
                    else interaction.message.id
                    if interaction is not None and interaction.message is not None
                    else None
                ),
                "interaction_id": None if interaction is None else interaction.id,
                "button_custom_id": button_custom_id,
                "raw_user_input": raw_user_input or "",
                "normalized_user_input": normalized_user_input or "",
                "selected_product_key": None if product is None else product.key,
                "selected_product_label": None if product is None else product.label,
                "selected_product_filename": (
                    None if product is None else product.file_path.name
                ),
                "selected_price": self.get_effective_ticket_price_text(
                    product,
                    ticket_record=ticket_record,
                ),
                "payment_platform_key": None if platform is None else platform.key,
                "payment_platform_label": None if platform is None else platform.label,
                "payment_note_code": payment_note_code or "",
                "delivery_filename": delivery_filename or "",
                "gmail_message_id": gmail_message_id or "",
                "purchase_event_id": purchase_event_id or "",
                "failure_reason": failure_reason or "",
                "error_type": "" if error is None else type(error).__name__,
                "error_message": "" if error is None else str(error),
                "details": details_payload,
            }
            self.audit_logger.log_event(event)
        except Exception:
            self.logger.exception(
                "purchase_audit_event_failed event_type=%s timestamp=%s",
                event_type,
                utc_timestamp(),
            )

    async def audit_stage_transition(
        self,
        *,
        trigger: str,
        channel: discord.TextChannel | None = None,
        interaction: discord.Interaction | None = None,
        message: discord.Message | None = None,
        actor_user_id: int | None = None,
        ticket_owner_id: int | None = None,
        ticket_record: TicketRecord | None = None,
        previous_ticket_stage: str | None,
        next_ticket_stage: str | None,
        product: ScriptProduct | None = None,
        platform: PaymentPlatform | None = None,
        payment_note_code: str | None = None,
        button_custom_id: str | None = None,
        raw_user_input: str | None = None,
        normalized_user_input: str | None = None,
        failure_reason: str | None = None,
        error: BaseException | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        if previous_ticket_stage == next_ticket_stage:
            return

        await self.audit_purchase_event(
            "ticket_stage_transition",
            event_category="state",
            status="success" if error is None else "failure",
            trigger=trigger,
            channel=channel,
            interaction=interaction,
            message=message,
            actor_user_id=actor_user_id,
            ticket_owner_id=ticket_owner_id,
            ticket_record=ticket_record,
            ticket_stage=next_ticket_stage,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=next_ticket_stage,
            product=product,
            platform=platform,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
            raw_user_input=raw_user_input,
            normalized_user_input=normalized_user_input,
            failure_reason=failure_reason,
            error=error,
            details=details,
        )

    async def report_purchase_flow_exception(
        self,
        *,
        event_type: str,
        trigger: str,
        error: BaseException,
        channel: discord.TextChannel | None = None,
        interaction: discord.Interaction | None = None,
        message: discord.Message | None = None,
        actor_user_id: int | None = None,
        ticket_owner_id: int | None = None,
        ticket_record: TicketRecord | None = None,
        button_custom_id: str | None = None,
        raw_user_input: str | None = None,
        normalized_user_input: str | None = None,
        failure_reason: str,
        details: dict[str, object] | None = None,
    ) -> None:
        try:
            resolved_channel = channel
            if resolved_channel is None and interaction is not None and isinstance(
                interaction.channel,
                discord.TextChannel,
            ):
                resolved_channel = interaction.channel
            if resolved_channel is None and message is not None and isinstance(
                message.channel,
                discord.TextChannel,
            ):
                resolved_channel = message.channel

            if button_custom_id is None and interaction is not None:
                button_custom_id = self.interaction_custom_id(interaction)

            if actor_user_id is None:
                if interaction is not None:
                    actor_user_id = interaction.user.id
                elif message is not None:
                    actor_user_id = message.author.id

            if (
                resolved_channel is not None
                and self.is_purchase_ticket_channel(resolved_channel)
                and ticket_owner_id is None
            ):
                try:
                    ticket_owner_id = await self.get_authoritative_ticket_owner_id(
                        resolved_channel
                    )
                except Exception:
                    self.logger.exception(
                        "purchase_flow_exception_owner_lookup_failed channel_id=%s event_type=%s timestamp=%s",
                        resolved_channel.id,
                        event_type,
                        utc_timestamp(),
                    )

            if (
                resolved_channel is not None
                and self.is_purchase_ticket_channel(resolved_channel)
                and ticket_record is None
            ):
                try:
                    ticket_record = await self.get_ticket_record_snapshot(
                        resolved_channel.id,
                        owner_id=ticket_owner_id,
                    )
                except Exception:
                    self.logger.exception(
                        "purchase_flow_exception_ticket_record_lookup_failed channel_id=%s event_type=%s timestamp=%s",
                        resolved_channel.id,
                        event_type,
                        utc_timestamp(),
                    )

            self.logger.error(
                "purchase_flow_exception event_type=%s trigger=%s channel_id=%s user_id=%s ticket_owner_id=%s button_custom_id=%s timestamp=%s",
                event_type,
                trigger,
                None if resolved_channel is None else resolved_channel.id,
                actor_user_id,
                ticket_owner_id,
                button_custom_id,
                utc_timestamp(),
                exc_info=(type(error), error, error.__traceback__),
            )
            await self.audit_purchase_event(
                event_type,
                event_category="exception",
                status="failure",
                trigger=trigger,
                channel=resolved_channel,
                interaction=interaction,
                message=message,
                actor_user_id=actor_user_id,
                ticket_owner_id=ticket_owner_id,
                ticket_record=ticket_record,
                button_custom_id=button_custom_id,
                raw_user_input=raw_user_input,
                normalized_user_input=normalized_user_input,
                failure_reason=failure_reason,
                error=error,
                details=details,
            )
        except Exception:
            self.logger.exception(
                "purchase_flow_exception_report_failed event_type=%s timestamp=%s",
                event_type,
                utc_timestamp(),
            )

    def cleanup_payment_task(self, completed: asyncio.Future[None]) -> None:
        if isinstance(completed, asyncio.Task):
            task = cast(asyncio.Task[None], completed)
            self.payment_check_tasks.discard(task)
            if task.cancelled():
                return
            try:
                task.result()
            except Exception:
                self.logger.exception(
                    "payment_check_task_failed timestamp=%s",
                    utc_timestamp(),
                )

    async def retry_pending_purchase_logs(self) -> None:
        async with self.purchase_sync_lock:
            try:
                synced_records, total_records = await asyncio.to_thread(
                    self.purchase_logger.retry_pending_records
                )
            except Exception:
                self.logger.exception(
                    "purchase_sync_retry_failed timestamp=%s",
                    utc_timestamp(),
                )
                return

            if total_records > 0:
                self.logger.info(
                    "purchase_sync_retry_completed timestamp=%s synced_records=%s total_records=%s remaining_records=%s",
                    utc_timestamp(),
                    synced_records,
                    total_records,
                    total_records - synced_records,
                )

    async def purchase_sync_retry_loop(self) -> None:
        try:
            while not self.is_closed():
                await self.retry_pending_purchase_logs()
                await asyncio.sleep(PURCHASE_SYNC_RETRY_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            return

    async def get_existing_ticket_record(
        self,
        channel_id: int,
    ) -> TicketRecord | None:
        async with self.state_lock:
            tickets = self.state.get("tickets")
            if not isinstance(tickets, dict):
                return None
            record = tickets.get(str(channel_id))
            if not isinstance(record, dict):
                return None
            return cast(TicketRecord, dict(record))

    async def remove_ticket_record(self, channel_id: int) -> None:
        async with self.state_lock:
            tickets = self.state.get("tickets")
            if not isinstance(tickets, dict):
                return
            if tickets.pop(str(channel_id), None) is not None:
                self.persist_state()

    def parse_utc_datetime(self, raw_value: object) -> datetime | None:
        if not isinstance(raw_value, str):
            return None
        stripped_value = raw_value.strip()
        if not stripped_value:
            return None

        try:
            parsed = datetime.fromisoformat(stripped_value)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def build_purchase_ticket_auto_close_deadline(
        self,
        *,
        now_utc: datetime | None = None,
    ) -> str:
        scheduled_time = (
            now_utc or datetime.now(timezone.utc)
        ) + timedelta(seconds=PURCHASE_TICKET_AUTO_CLOSE_DELAY_SECONDS)
        return scheduled_time.isoformat()

    def cancel_purchase_ticket_auto_close_task(self, channel_id: int) -> None:
        task = self.purchase_ticket_auto_close_tasks.pop(channel_id, None)
        if task is not None:
            task.cancel()

    def cleanup_purchase_ticket_auto_close_task(
        self,
        channel_id: int,
        completed: asyncio.Future[None],
    ) -> None:
        current_task = self.purchase_ticket_auto_close_tasks.get(channel_id)
        if current_task is completed:
            self.purchase_ticket_auto_close_tasks.pop(channel_id, None)
        if completed.cancelled():
            return
        try:
            completed.result()
        except Exception:
            self.logger.exception(
                "purchase_ticket_auto_close_task_failed channel_id=%s timestamp=%s",
                channel_id,
                utc_timestamp(),
            )

    async def schedule_purchase_ticket_auto_close(
        self,
        channel: discord.TextChannel,
        *,
        auto_close_at_utc: str | None = None,
    ) -> str | None:
        resolved_auto_close_at_utc = (
            auto_close_at_utc or self.build_purchase_ticket_auto_close_deadline()
        )
        if self.parse_utc_datetime(resolved_auto_close_at_utc) is None:
            return None

        await self.update_ticket_record(
            channel.id,
            auto_close_at_utc=resolved_auto_close_at_utc,
        )
        self.cancel_purchase_ticket_auto_close_task(channel.id)
        task = asyncio.create_task(
            self.run_purchase_ticket_auto_close(
                channel.id,
                auto_close_at_utc=resolved_auto_close_at_utc,
            )
        )
        self.purchase_ticket_auto_close_tasks[channel.id] = task
        task.add_done_callback(
            lambda completed, channel_id=channel.id: self.cleanup_purchase_ticket_auto_close_task(
                channel_id,
                completed,
            )
        )
        return resolved_auto_close_at_utc

    async def restore_purchase_ticket_auto_close_tasks(self) -> None:
        async with self.state_lock:
            tickets = self.state.get("tickets")
            saved_records = (
                [
                    (channel_id, cast(TicketRecord, dict(record)))
                    for channel_id, record in tickets.items()
                    if isinstance(channel_id, str) and isinstance(record, dict)
                ]
                if isinstance(tickets, dict)
                else []
            )

        for channel_id_value, ticket_record in saved_records:
            if cast(str | None, ticket_record.get("stage")) != TICKET_STAGE_COMPLETED:
                continue

            try:
                channel_id = int(channel_id_value)
            except ValueError:
                continue

            auto_close_at_utc = cast(str | None, ticket_record.get("auto_close_at_utc"))
            if not auto_close_at_utc:
                auto_close_at_utc = self.build_purchase_ticket_auto_close_deadline()

            channel = self.get_channel(channel_id)
            if not isinstance(channel, discord.TextChannel):
                try:
                    fetched_channel = await self.fetch_channel(channel_id)
                except discord.NotFound:
                    await self.remove_ticket_record(channel_id)
                    continue
                except discord.DiscordException:
                    self.logger.exception(
                        "purchase_ticket_auto_close_channel_fetch_failed channel_id=%s timestamp=%s",
                        channel_id,
                        utc_timestamp(),
                    )
                    continue
                if not isinstance(fetched_channel, discord.TextChannel):
                    continue
                channel = fetched_channel

            if not self.is_purchase_ticket_channel(channel):
                continue

            await self.schedule_purchase_ticket_auto_close(
                channel,
                auto_close_at_utc=auto_close_at_utc,
            )

    async def close_purchase_ticket_channel(
        self,
        channel: discord.TextChannel,
        *,
        delete_reason: str,
        closing_message: str | None = None,
        grace_period_seconds: int = 0,
        cancel_scheduled_close: bool = True,
    ) -> bool:
        if cancel_scheduled_close:
            self.cancel_purchase_ticket_auto_close_task(channel.id)

        if closing_message:
            try:
                await channel.send(
                    closing_message,
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "purchase_ticket_close_notice_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )

        if grace_period_seconds > 0:
            await asyncio.sleep(grace_period_seconds)

        try:
            await channel.delete(reason=delete_reason)
        except discord.NotFound:
            await self.remove_ticket_record(channel.id)
            self.logger.info(
                "purchase_ticket_channel_already_deleted channel_id=%s timestamp=%s",
                channel.id,
                utc_timestamp(),
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "purchase_ticket_channel_delete_failed channel_id=%s timestamp=%s",
                channel.id,
                utc_timestamp(),
            )
            return False

        await self.remove_ticket_record(channel.id)
        self.logger.info(
            "purchase_ticket_channel_deleted channel_id=%s timestamp=%s",
            channel.id,
            utc_timestamp(),
        )
        return True

    async def close_support_ticket_channel(
        self,
        channel: discord.TextChannel,
        *,
        delete_reason: str,
        closing_message: str | None = None,
        grace_period_seconds: int = 0,
    ) -> bool:
        if closing_message:
            try:
                await channel.send(
                    closing_message,
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "support_ticket_close_notice_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )

        if grace_period_seconds > 0:
            await asyncio.sleep(grace_period_seconds)

        try:
            await channel.delete(reason=delete_reason)
        except discord.NotFound:
            self.logger.info(
                "support_ticket_channel_already_deleted channel_id=%s timestamp=%s",
                channel.id,
                utc_timestamp(),
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "support_ticket_channel_delete_failed channel_id=%s timestamp=%s",
                channel.id,
                utc_timestamp(),
            )
            return False

        self.logger.info(
            "support_ticket_channel_deleted channel_id=%s timestamp=%s",
            channel.id,
            utc_timestamp(),
        )
        return True

    async def run_purchase_ticket_auto_close(
        self,
        channel_id: int,
        *,
        auto_close_at_utc: str,
    ) -> None:
        auto_close_at = self.parse_utc_datetime(auto_close_at_utc)
        if auto_close_at is None:
            return

        remaining_seconds = (
            auto_close_at - datetime.now(timezone.utc)
        ).total_seconds()
        if remaining_seconds > 0:
            await asyncio.sleep(remaining_seconds)

        ticket_record = await self.get_existing_ticket_record(channel_id)
        if ticket_record is None:
            return

        if cast(str | None, ticket_record.get("stage")) != TICKET_STAGE_COMPLETED:
            return

        if cast(str | None, ticket_record.get("auto_close_at_utc")) != auto_close_at_utc:
            return

        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched_channel = await self.fetch_channel(channel_id)
            except discord.NotFound:
                await self.remove_ticket_record(channel_id)
                return
            except discord.DiscordException:
                self.logger.exception(
                    "purchase_ticket_auto_close_channel_fetch_failed channel_id=%s timestamp=%s",
                    channel_id,
                    utc_timestamp(),
                )
                return
            if not isinstance(fetched_channel, discord.TextChannel):
                return
            channel = fetched_channel

        if not self.is_purchase_ticket_channel(channel):
            return

        owner_id = cast(int | None, ticket_record.get("owner_id"))
        product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        closed_ok = await self.close_purchase_ticket_channel(
            channel,
            delete_reason="Auto-close completed purchase ticket after delivery window",
            closing_message=(
                "This purchase ticket is now closing automatically. "
                "You can open a new ticket from the panel any time you want another script."
            ),
            grace_period_seconds=5,
            cancel_scheduled_close=False,
        )
        await self.audit_purchase_event(
            "purchase_ticket_auto_closed",
            event_category="ticket",
            status="success" if closed_ok else "failure",
            trigger="scheduled_task",
            channel=channel,
            actor_user_id=owner_id,
            ticket_owner_id=owner_id,
            ticket_record=ticket_record,
            product=product,
            failure_reason="" if closed_ok else "automatic purchase ticket close failed",
            details={"auto_close_at_utc": auto_close_at_utc},
        )

    async def get_authoritative_ticket_owner_id(
        self,
        channel: discord.TextChannel,
        *,
        fallback_owner_id: int | None = None,
    ) -> int | None:
        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        state_owner_id = ticket_record.get("owner_id")
        authoritative_owner_id = (
            state_owner_id
            if isinstance(state_owner_id, int) and not isinstance(state_owner_id, bool)
            else None
        )
        topic_owner_id = ticket_owner_id_from_topic(channel.topic)

        if authoritative_owner_id is not None:
            if topic_owner_id is not None and topic_owner_id != authoritative_owner_id:
                self.logger.warning(
                    "ticket_owner_topic_mismatch channel_id=%s state_owner_id=%s topic_owner_id=%s timestamp=%s",
                    channel.id,
                    authoritative_owner_id,
                    topic_owner_id,
                    utc_timestamp(),
                )
            return authoritative_owner_id

        if topic_owner_id is not None:
            await self.update_ticket_record(channel.id, owner_id=topic_owner_id)
            return topic_owner_id

        overwrite_owner_id = self.infer_ticket_owner_id_from_overwrites(channel)
        if overwrite_owner_id is not None:
            await self.update_ticket_record(channel.id, owner_id=overwrite_owner_id)
            return overwrite_owner_id

        if fallback_owner_id is not None:
            self.logger.warning(
                "ticket_owner_fallback_used channel_id=%s fallback_owner_id=%s timestamp=%s",
                channel.id,
                fallback_owner_id,
                utc_timestamp(),
            )
            await self.update_ticket_record(channel.id, owner_id=fallback_owner_id)
            return fallback_owner_id

        return None

    async def get_ticket_record_snapshot(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
    ) -> TicketRecord:
        async with self.state_lock:
            record_key = str(channel_id)
            tickets = self.state.get("tickets")
            if tickets is None:
                tickets = {}
                self.state["tickets"] = tickets

            changed = False
            record = tickets.get(record_key)
            if record is None:
                record = fresh_ticket_record(owner_id)
                tickets[record_key] = record
                changed = True
            elif owner_id is not None and record.get("owner_id") is None:
                record["owner_id"] = owner_id
                changed = True

            if changed:
                self.persist_state()
            return cast(TicketRecord, dict(record))

    async def update_ticket_record(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
        selected_script_key: object = UNSET,
        ticket_price_override: object = UNSET,
        payment_platform_key: object = UNSET,
        payment_note_code: object = UNSET,
        auto_close_at_utc: object = UNSET,
        stage: str | None = None,
    ) -> TicketRecord:
        if stage is not None and stage != TICKET_STAGE_COMPLETED:
            self.cancel_purchase_ticket_auto_close_task(channel_id)

        async with self.state_lock:
            record = get_ticket_record(
                self.state,
                str(channel_id),
                owner_id=owner_id,
            )
            if stage is not None:
                record["stage"] = stage
                if stage != TICKET_STAGE_COMPLETED and auto_close_at_utc is UNSET:
                    record["auto_close_at_utc"] = None
            if selected_script_key is not UNSET:
                record["selected_script_key"] = cast(str | None, selected_script_key)
            if ticket_price_override is not UNSET:
                record["ticket_price_override"] = cast(
                    str | None,
                    ticket_price_override,
                )
            if payment_platform_key is not UNSET:
                record["payment_platform_key"] = cast(
                    str | None,
                    payment_platform_key,
                )
            if payment_note_code is not UNSET:
                record["payment_note_code"] = cast(str | None, payment_note_code)
            if auto_close_at_utc is not UNSET:
                record["auto_close_at_utc"] = cast(str | None, auto_close_at_utc)
            self.persist_state()
            return cast(TicketRecord, dict(record))

    async def ensure_payment_note_code(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
    ) -> str:
        ticket_record = await self.get_ticket_record_snapshot(channel_id)
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        if payment_note_code:
            return payment_note_code

        payment_note_code = generate_payment_note_code()
        await self.update_ticket_record(
            channel_id,
            owner_id=owner_id,
            payment_note_code=payment_note_code,
        )
        return payment_note_code

    async def setup_hook(self) -> None:
        self.add_view(self.build_ticket_panel_view())
        self.add_view(self.build_support_ticket_panel_view())
        self.add_view(self.build_payment_platform_selection_view())
        self.add_view(self.build_payment_confirmation_view())
        self.logger.info("ticket_view_registered custom_id=%s", TICKET_BUTTON_CUSTOM_ID)
        self.logger.info(
            "support_ticket_view_registered custom_id=%s",
            SUPPORT_TICKET_BUTTON_CUSTOM_ID,
        )
        for platform in PAYMENT_PLATFORMS:
            self.logger.info(
                "payment_platform_view_registered custom_id=%s platform=%s",
                payment_platform_button_custom_id(platform.key),
                platform.key,
            )
        self.logger.info(
            "payment_view_registered custom_id=%s",
            PAYMENT_BUTTON_CUSTOM_ID,
        )

    async def on_ready(self) -> None:
        if self.user is None:
            return

        self.logger.info(
            "bot_ready user_id=%s username=%r state_source=%s timestamp=%s",
            self.user.id,
            str(self.user),
            self.state_load_source,
            utc_timestamp(),
        )
        print(f"Logged in as {self.user} ({self.user.id})")
        await self.ensure_ticket_panel()
        await self.ensure_support_ticket_panel()
        await self.retry_pending_purchase_logs()
        await self.restore_purchase_ticket_auto_close_tasks()
        if self.purchase_sync_retry_task is None or self.purchase_sync_retry_task.done():
            self.purchase_sync_retry_task = asyncio.create_task(
                self.purchase_sync_retry_loop()
            )
        if self.weekly_sales_report_task is None or self.weekly_sales_report_task.done():
            self.weekly_sales_report_task = asyncio.create_task(
                self.weekly_sales_report_loop()
            )

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        interaction_type = getattr(interaction.type, "name", str(interaction.type))
        custom_id: str | None = None
        if isinstance(interaction.data, dict):
            custom_id_value = cast(object, interaction.data.get("custom_id"))
            if isinstance(custom_id_value, str):
                custom_id = custom_id_value

        self.logger.info(
            "interaction_received type=%s custom_id=%r user_id=%s channel_id=%s timestamp=%s",
            interaction_type,
            custom_id,
            getattr(interaction.user, "id", None),
            getattr(interaction.channel, "id", None),
            utc_timestamp(),
        )

    async def get_ticket_panel_channel(self) -> discord.TextChannel | None:
        channel = self.get_channel(TICKET_PANEL_CHANNEL_ID)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(TICKET_PANEL_CHANNEL_ID)
        except discord.DiscordException:
            self.logger.exception(
                "ticket_panel_channel_fetch_failed channel_id=%s timestamp=%s",
                TICKET_PANEL_CHANNEL_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.TextChannel):
            return fetched_channel

        self.logger.error(
            "ticket_panel_channel_invalid_type channel_id=%s timestamp=%s",
            TICKET_PANEL_CHANNEL_ID,
            utc_timestamp(),
        )
        return None

    async def get_support_ticket_panel_channel(self) -> discord.TextChannel | None:
        channel = self.get_channel(SUPPORT_TICKET_PANEL_CHANNEL_ID)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(SUPPORT_TICKET_PANEL_CHANNEL_ID)
        except discord.DiscordException:
            self.logger.exception(
                "support_ticket_panel_channel_fetch_failed channel_id=%s timestamp=%s",
                SUPPORT_TICKET_PANEL_CHANNEL_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.TextChannel):
            return fetched_channel

        self.logger.error(
            "support_ticket_panel_channel_invalid_type channel_id=%s timestamp=%s",
            SUPPORT_TICKET_PANEL_CHANNEL_ID,
            utc_timestamp(),
        )
        return None

    async def get_ticket_category(self) -> discord.CategoryChannel | None:
        channel = self.get_channel(TICKET_CATEGORY_ID)
        if isinstance(channel, discord.CategoryChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(TICKET_CATEGORY_ID)
        except discord.DiscordException:
            self.logger.exception(
                "ticket_category_fetch_failed category_id=%s timestamp=%s",
                TICKET_CATEGORY_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.CategoryChannel):
            return fetched_channel

        self.logger.error(
            "ticket_category_invalid_type category_id=%s timestamp=%s",
            TICKET_CATEGORY_ID,
            utc_timestamp(),
        )
        return None

    async def get_support_ticket_category(self) -> discord.CategoryChannel | None:
        channel = self.get_channel(SUPPORT_TICKET_CATEGORY_ID)
        if isinstance(channel, discord.CategoryChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(SUPPORT_TICKET_CATEGORY_ID)
        except discord.DiscordException:
            self.logger.exception(
                "support_ticket_category_fetch_failed category_id=%s timestamp=%s",
                SUPPORT_TICKET_CATEGORY_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.CategoryChannel):
            return fetched_channel

        self.logger.error(
            "support_ticket_category_invalid_type category_id=%s timestamp=%s",
            SUPPORT_TICKET_CATEGORY_ID,
            utc_timestamp(),
        )
        return None

    async def ensure_ticket_panel(self) -> None:
        channel = await self.get_ticket_panel_channel()
        if channel is None:
            return

        async with self.ticket_panel_lock:
            panel_message_text = build_ticket_panel_message()
            message_id = self.state.get("ticket_panel_message_id")
            stored_message: discord.Message | None = None

            if isinstance(message_id, int):
                try:
                    stored_message = await channel.fetch_message(message_id)
                except discord.NotFound:
                    stored_message = None
                except discord.DiscordException:
                    self.logger.exception(
                        "ticket_panel_message_fetch_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        message_id,
                        utc_timestamp(),
                    )
                    return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
                and message_has_component_custom_id(
                    stored_message,
                    TICKET_BUTTON_CUSTOM_ID,
                )
                and stored_message.content == panel_message_text
            ):
                return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
            ):
                try:
                    await stored_message.edit(
                        content=panel_message_text,
                        view=self.build_ticket_panel_view(),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    return
                except discord.DiscordException:
                    self.logger.exception(
                        "ticket_panel_message_edit_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        stored_message.id,
                        utc_timestamp(),
                    )
                    return

            if stored_message is not None:
                self.logger.warning(
                    "ticket_panel_message_unexpected_author channel_id=%s message_id=%s timestamp=%s",
                    channel.id,
                    stored_message.id,
                    utc_timestamp(),
                )
                return

            try:
                panel_message = await channel.send(
                    panel_message_text,
                    view=self.build_ticket_panel_view(),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "ticket_panel_message_send_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )
                return

            async with self.state_lock:
                self.state["ticket_panel_message_id"] = panel_message.id
                self.persist_state()

    async def ensure_support_ticket_panel(self) -> None:
        channel = await self.get_support_ticket_panel_channel()
        if channel is None:
            return

        async with self.support_ticket_panel_lock:
            panel_message_text = build_support_ticket_panel_message()
            message_id = self.state.get("support_ticket_panel_message_id")
            stored_message: discord.Message | None = None

            if isinstance(message_id, int):
                try:
                    stored_message = await channel.fetch_message(message_id)
                except discord.NotFound:
                    stored_message = None
                except discord.DiscordException:
                    self.logger.exception(
                        "support_ticket_panel_message_fetch_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        message_id,
                        utc_timestamp(),
                    )
                    return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
                and message_has_component_custom_id(
                    stored_message,
                    SUPPORT_TICKET_BUTTON_CUSTOM_ID,
                )
                and stored_message.content == panel_message_text
            ):
                return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
            ):
                try:
                    await stored_message.edit(
                        content=panel_message_text,
                        view=self.build_support_ticket_panel_view(),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    return
                except discord.DiscordException:
                    self.logger.exception(
                        "support_ticket_panel_message_edit_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        stored_message.id,
                        utc_timestamp(),
                    )
                    return

            if stored_message is not None:
                self.logger.warning(
                    "support_ticket_panel_message_unexpected_author channel_id=%s message_id=%s timestamp=%s",
                    channel.id,
                    stored_message.id,
                    utc_timestamp(),
                )
                return

            try:
                panel_message = await channel.send(
                    panel_message_text,
                    view=self.build_support_ticket_panel_view(),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "support_ticket_panel_message_send_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )
                return

            async with self.state_lock:
                self.state["support_ticket_panel_message_id"] = panel_message.id
                self.persist_state()

    def is_purchase_ticket_channel(self, channel: discord.abc.Messageable) -> bool:
        return (
            isinstance(channel, discord.TextChannel)
            and channel.category_id == TICKET_CATEGORY_ID
            and channel.name.startswith("ticket-")
        )

    def is_support_ticket_channel(self, channel: discord.abc.Messageable) -> bool:
        return (
            isinstance(channel, discord.TextChannel)
            and channel.category_id == SUPPORT_TICKET_CATEGORY_ID
            and channel.name.startswith("support-ticket-")
        )

    def infer_ticket_owner_id_from_overwrites(
        self,
        channel: discord.TextChannel,
    ) -> int | None:
        bot_user_id = self.user.id if self.user is not None else None
        candidate_owner_ids: list[int] = []

        for target, overwrite in channel.overwrites.items():
            member_id: int | None = None
            if isinstance(target, discord.Member):
                member_id = target.id
            elif isinstance(target, discord.Object):
                member = channel.guild.get_member(target.id)
                if member is not None:
                    member_id = member.id

            if member_id is None or member_id == bot_user_id:
                continue
            if overwrite.view_channel is False:
                continue
            if member_id not in candidate_owner_ids:
                candidate_owner_ids.append(member_id)

        if len(candidate_owner_ids) == 1:
            return candidate_owner_ids[0]

        if len(candidate_owner_ids) > 1:
            self.logger.warning(
                "ticket_owner_permission_overwrites_ambiguous channel_id=%s candidate_owner_ids=%s timestamp=%s",
                channel.id,
                candidate_owner_ids,
                utc_timestamp(),
            )
        return None

    def find_existing_ticket_channel(
        self,
        category: discord.CategoryChannel,
        user_id: int,
    ) -> discord.TextChannel | None:
        expected_topic = ticket_owner_topic(user_id)
        for channel in category.text_channels:
            if (
                channel.topic == expected_topic
                or self.infer_ticket_owner_id_from_overwrites(channel) == user_id
            ):
                return channel
        return None

    async def find_existing_purchase_ticket_channel(
        self,
        category: discord.CategoryChannel,
        user_id: int,
    ) -> discord.TextChannel | None:
        expected_topic = ticket_owner_topic(user_id)
        for channel in category.text_channels:
            if (
                channel.topic != expected_topic
                and self.infer_ticket_owner_id_from_overwrites(channel) != user_id
            ):
                continue

            ticket_record = await self.get_existing_ticket_record(channel.id)
            if ticket_record is None:
                return channel

            ticket_stage = cast(str | None, ticket_record.get("stage"))
            if ticket_stage == TICKET_STAGE_COMPLETED:
                auto_close_at_utc = cast(
                    str | None,
                    ticket_record.get("auto_close_at_utc"),
                )
                await self.schedule_purchase_ticket_auto_close(
                    channel,
                    auto_close_at_utc=auto_close_at_utc,
                )
                continue

            return channel

        return None

    def get_ticket_support_roles(self, guild: discord.Guild) -> list[discord.Role]:
        support_roles: list[discord.Role] = []
        for role in guild.roles:
            permissions = role.permissions
            if (
                permissions.administrator
                or permissions.manage_guild
                or permissions.manage_channels
                or permissions.manage_messages
                or permissions.moderate_members
                or permissions.kick_members
                or permissions.ban_members
            ):
                support_roles.append(role)
        return support_roles

    def build_unique_ticket_channel_name(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> str:
        base_name = build_ticket_channel_name(member.display_name or member.name)
        return self.build_unique_channel_name(category, member.id, base_name)

    def build_unique_support_ticket_channel_name(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> str:
        base_name = build_support_ticket_channel_name(member.display_name or member.name)
        return self.build_unique_channel_name(category, member.id, base_name)

    def build_unique_channel_name(
        self,
        category: discord.CategoryChannel,
        user_id: int,
        base_name: str,
    ) -> str:
        existing_names = {channel.name for channel in category.text_channels}
        if base_name not in existing_names:
            return base_name

        base_suffix = str(user_id)[-4:]
        for attempt in range(100):
            suffix = base_suffix if attempt == 0 else f"{base_suffix}-{attempt + 1}"
            trimmed_base = base_name[: max(1, 99 - len(suffix) - 1)]
            candidate_name = f"{trimmed_base}-{suffix}"
            if candidate_name not in existing_names:
                return candidate_name

        fallback_suffix = str(user_id)
        trimmed_base = base_name[: max(1, 99 - len(fallback_suffix) - 1)]
        return f"{trimmed_base}-{fallback_suffix}"

    def get_support_moderator_role(self, guild: discord.Guild) -> discord.Role | None:
        return guild.get_role(SUPPORT_MODERATOR_ROLE_ID)

    async def create_ticket_channel(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> discord.TextChannel:
        bot_member = category.guild.me
        if bot_member is None and self.user is not None:
            bot_member = category.guild.get_member(self.user.id)
        if bot_member is None:
            raise RuntimeError("Unable to resolve the bot member for ticket permissions.")

        overwrites: dict[
            discord.Role | discord.Member | discord.Object,
            discord.PermissionOverwrite,
        ] = {
            category.guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            ),
        }

        for role in self.get_ticket_support_roles(category.guild):
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        channel_name = self.build_unique_ticket_channel_name(category, member)
        channel = await category.guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=ticket_owner_topic(member.id),
            reason=f"Purchase ticket opened by {member} ({member.id})",
        )
        await channel.send(
            build_ticket_store_message(member.display_name or member.name),
            allowed_mentions=self.response_allowed_mentions,
        )
        await self.update_ticket_record(
            channel.id,
            owner_id=member.id,
            selected_script_key=None,
            stage=TICKET_STAGE_AWAITING_SELECTION,
        )
        return channel

    async def create_support_ticket_channel(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> discord.TextChannel:
        bot_member = category.guild.me
        if bot_member is None and self.user is not None:
            bot_member = category.guild.get_member(self.user.id)
        if bot_member is None:
            raise RuntimeError("Unable to resolve the bot member for ticket permissions.")

        overwrites: dict[
            discord.Role | discord.Member | discord.Object,
            discord.PermissionOverwrite,
        ] = {
            category.guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            ),
        }

        for role in self.get_ticket_support_roles(category.guild):
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        moderator_role = self.get_support_moderator_role(category.guild)
        if moderator_role is None:
            self.logger.warning(
                "support_moderator_role_missing guild_id=%s role_id=%s timestamp=%s",
                category.guild.id,
                SUPPORT_MODERATOR_ROLE_ID,
                utc_timestamp(),
            )
        else:
            overwrites[moderator_role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        channel_name = self.build_unique_support_ticket_channel_name(category, member)
        channel = await category.guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=ticket_owner_topic(member.id),
            reason=f"Support ticket opened by {member} ({member.id})",
        )
        await channel.send(
            SUPPORT_TICKET_CHANNEL_MESSAGE,
            allowed_mentions=self.support_ping_allowed_mentions,
        )
        return channel

    async def handle_ticket_button(self, interaction: discord.Interaction) -> None:
        button_custom_id = self.interaction_custom_id(interaction)
        await self.audit_purchase_event(
            "ticket_open_button_pressed",
            event_category="interaction",
            status="success",
            trigger="button_press",
            interaction=interaction,
            button_custom_id=button_custom_id,
        )
        guild = interaction.guild
        if guild is None:
            await self.audit_purchase_event(
                "ticket_open_rejected",
                event_category="ticket",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="interaction is not in a guild",
            )
            await interaction.response.send_message(
                "Tickets can only be opened inside a server.",
                ephemeral=True,
            )
            return

        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None:
            member = guild.get_member(interaction.user.id)
        if member is None:
            await self.audit_purchase_event(
                "ticket_open_rejected",
                event_category="ticket",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="member lookup failed",
            )
            await interaction.response.send_message(
                "I couldn't resolve your server membership. Please try again.",
                ephemeral=True,
            )
            return

        category = await self.get_ticket_category()
        if category is None or category.guild.id != guild.id:
            await self.audit_purchase_event(
                "ticket_open_rejected",
                event_category="ticket",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                actor_user_id=member.id,
                button_custom_id=button_custom_id,
                failure_reason="ticket category unavailable",
            )
            await interaction.response.send_message(
                "The ticket category is unavailable right now. Please contact a moderator.",
                ephemeral=True,
            )
            return

        async with self.ticket_creation_lock:
            existing_channel = await self.find_existing_purchase_ticket_channel(
                category,
                member.id,
            )
            if existing_channel is not None:
                await self.audit_purchase_event(
                    "ticket_open_existing",
                    event_category="ticket",
                    status="ignored",
                    trigger="button_press",
                    channel=existing_channel,
                    interaction=interaction,
                    actor_user_id=member.id,
                    ticket_owner_id=member.id,
                    button_custom_id=button_custom_id,
                    details={
                        "existing_channel_id": existing_channel.id,
                        "existing_channel_name": existing_channel.name,
                    },
                )
                await interaction.response.send_message(
                    f"You already have an open ticket: {existing_channel.mention}",
                    ephemeral=True,
                )
                return

            try:
                ticket_channel = await self.create_ticket_channel(category, member)
            except Exception as exc:
                self.logger.exception(
                    "ticket_channel_create_failed guild_id=%s user_id=%s timestamp=%s",
                    guild.id,
                    member.id,
                    utc_timestamp(),
                )
                await self.audit_purchase_event(
                    "ticket_open_failed",
                    event_category="ticket",
                    status="failure",
                    trigger="button_press",
                    interaction=interaction,
                    actor_user_id=member.id,
                    ticket_owner_id=member.id,
                    button_custom_id=button_custom_id,
                    error=exc,
                    failure_reason="ticket channel creation failed",
                )
                await interaction.response.send_message(
                    "I couldn't create your ticket right now. Please try again shortly.",
                    ephemeral=True,
                )
                return

        self.logger.info(
            "ticket_channel_created guild_id=%s channel_id=%s user_id=%s timestamp=%s",
            guild.id,
            ticket_channel.id,
            member.id,
            utc_timestamp(),
        )
        ticket_record = await self.get_ticket_record_snapshot(
            ticket_channel.id,
            owner_id=member.id,
        )
        await self.audit_purchase_event(
            "ticket_opened",
            event_category="ticket",
            status="success",
            trigger="button_press",
            channel=ticket_channel,
            interaction=interaction,
            actor_user_id=member.id,
            ticket_owner_id=member.id,
            ticket_record=ticket_record,
            ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
            button_custom_id=button_custom_id,
        )
        await self.audit_stage_transition(
            trigger="button_press",
            channel=ticket_channel,
            interaction=interaction,
            actor_user_id=member.id,
            ticket_owner_id=member.id,
            ticket_record=ticket_record,
            previous_ticket_stage=None,
            next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
            button_custom_id=button_custom_id,
        )
        await interaction.response.send_message(
            f"Your ticket is ready: {ticket_channel.mention}",
            ephemeral=True,
        )

    async def handle_support_ticket_button(self, interaction: discord.Interaction) -> None:
        button_custom_id = self.interaction_custom_id(interaction)
        await self.audit_purchase_event(
            "support_ticket_button_pressed",
            event_category="interaction",
            status="success",
            trigger="button_press",
            interaction=interaction,
            button_custom_id=button_custom_id,
        )
        guild = interaction.guild
        if guild is None:
            await self.audit_purchase_event(
                "support_ticket_open_rejected",
                event_category="support",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="interaction is not in a guild",
            )
            await interaction.response.send_message(
                "Tickets can only be opened inside a server.",
                ephemeral=True,
            )
            return

        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None:
            member = guild.get_member(interaction.user.id)
        if member is None:
            await self.audit_purchase_event(
                "support_ticket_open_rejected",
                event_category="support",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="member lookup failed",
            )
            await interaction.response.send_message(
                "I couldn't resolve your server membership. Please try again.",
                ephemeral=True,
            )
            return

        category = await self.get_support_ticket_category()
        if category is None or category.guild.id != guild.id:
            await self.audit_purchase_event(
                "support_ticket_open_rejected",
                event_category="support",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                actor_user_id=member.id,
                button_custom_id=button_custom_id,
                failure_reason="support ticket category unavailable",
            )
            await interaction.response.send_message(
                "The support ticket category is unavailable right now. Please contact a moderator.",
                ephemeral=True,
            )
            return

        async with self.support_ticket_creation_lock:
            existing_channel = self.find_existing_ticket_channel(category, member.id)
            if existing_channel is not None:
                await self.audit_purchase_event(
                    "support_ticket_open_existing",
                    event_category="support",
                    status="ignored",
                    trigger="button_press",
                    channel=existing_channel,
                    interaction=interaction,
                    actor_user_id=member.id,
                    ticket_owner_id=member.id,
                    button_custom_id=button_custom_id,
                    details={
                        "existing_channel_id": existing_channel.id,
                        "existing_channel_name": existing_channel.name,
                    },
                )
                await interaction.response.send_message(
                    f"You already have an open support ticket: {existing_channel.mention}",
                    ephemeral=True,
                )
                return

            try:
                ticket_channel = await self.create_support_ticket_channel(category, member)
            except Exception as exc:
                self.logger.exception(
                    "support_ticket_channel_create_failed guild_id=%s user_id=%s timestamp=%s",
                    guild.id,
                    member.id,
                    utc_timestamp(),
                )
                await self.audit_purchase_event(
                    "support_ticket_open_failed",
                    event_category="support",
                    status="failure",
                    trigger="button_press",
                    interaction=interaction,
                    actor_user_id=member.id,
                    ticket_owner_id=member.id,
                    button_custom_id=button_custom_id,
                    error=exc,
                    failure_reason="support ticket channel creation failed",
                )
                await interaction.response.send_message(
                    "I couldn't create your support ticket right now. Please try again shortly.",
                    ephemeral=True,
                )
                return

        opened_at_utc = utc_timestamp()
        self.logger.info(
            "support_ticket_channel_created guild_id=%s channel_id=%s user_id=%s timestamp=%s",
            guild.id,
            ticket_channel.id,
            member.id,
            opened_at_utc,
        )
        await self.audit_purchase_event(
            "support_ticket_opened",
            event_category="support",
            status="success",
            trigger="button_press",
            channel=ticket_channel,
            interaction=interaction,
            actor_user_id=member.id,
            ticket_owner_id=member.id,
            button_custom_id=button_custom_id,
        )
        await self.audit_purchase_event(
            "support_escalation_triggered",
            event_category="support",
            status="success",
            trigger="button_press",
            channel=ticket_channel,
            interaction=interaction,
            actor_user_id=member.id,
            ticket_owner_id=member.id,
            button_custom_id=button_custom_id,
            details={
                "support_channel_id": ticket_channel.id,
                "support_channel_name": ticket_channel.name,
            },
        )
        await interaction.response.send_message(
            f"Your support ticket is ready: {ticket_channel.mention}",
            ephemeral=True,
        )
        await self.send_support_ticket_alert(
            ticket_channel,
            member,
            opened_at_utc=opened_at_utc,
        )

    async def handle_payment_button(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        button_custom_id = self.interaction_custom_id(interaction)
        await self.audit_purchase_event(
            "confirm_payment_button_pressed",
            event_category="interaction",
            status="success",
            trigger="button_press",
            interaction=interaction,
            button_custom_id=button_custom_id,
        )
        if not isinstance(channel, discord.TextChannel) or not self.is_purchase_ticket_channel(channel):
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="button used outside purchase ticket channel",
            )
            await interaction.response.send_message(
                "This button only works inside a ticket channel.",
                ephemeral=True,
            )
            return

        owner_id = await self.get_authoritative_ticket_owner_id(channel)
        if owner_id is None:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="ticket owner could not be resolved",
            )
            await interaction.response.send_message(
                "I couldn't verify the ticket owner from saved state. Please open a support ticket from the support panel if this keeps happening.",
                ephemeral=True,
            )
            return

        if owner_id is not None and interaction.user.id != owner_id:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                button_custom_id=button_custom_id,
                failure_reason="non-owner attempted to confirm payment",
            )
            await interaction.response.send_message(
                "Only the ticket owner can confirm payment here.",
                ephemeral=True,
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(
            channel.id,
        )
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )

        if ticket_stage == TICKET_STAGE_COMPLETED:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="ignored",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                button_custom_id=button_custom_id,
                failure_reason="ticket already completed",
            )
            await interaction.response.send_message(
                "Payment has already been confirmed for this ticket.",
                ephemeral=True,
            )
            return

        if ticket_stage == TICKET_STAGE_AWAITING_PAYMENT_PLATFORM:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                button_custom_id=button_custom_id,
                failure_reason="payment platform not chosen",
            )
            await interaction.response.send_message(
                "Choose the payment platform first. That step shows the exact payment instructions and note code you need before checking payment.",
                ephemeral=True,
            )
            return

        if selected_product is None or ticket_stage not in {
            TICKET_STAGE_AWAITING_PAYMENT,
            TICKET_STAGE_PAYMENT_PENDING,
        }:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                button_custom_id=button_custom_id,
                failure_reason="script selection not ready for payment confirmation",
            )
            await interaction.response.send_message(
                "Confirm your script selection first. After that, I will show the payment platform button.",
                ephemeral=True,
            )
            return

        if not payment_note_code:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                button_custom_id=button_custom_id,
                failure_reason="payment note code missing",
            )
            await interaction.response.send_message(
                "This ticket is missing its required payment note code. Press the payment platform button again to get the exact code before checking payment.",
                ephemeral=True,
            )
            return

        if channel.id in self.pending_payment_check_channel_ids:
            await self.audit_purchase_event(
                "confirm_payment_rejected",
                event_category="payment",
                status="ignored",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                payment_note_code=payment_note_code,
                button_custom_id=button_custom_id,
                failure_reason="payment check already running",
            )
            await interaction.response.send_message(
                self.build_payment_check_running_message(),
                ephemeral=True,
            )
            return

        previous_ticket_stage = ticket_stage
        await self.update_ticket_record(
            channel.id,
            owner_id=owner_id,
            stage=TICKET_STAGE_PAYMENT_PENDING,
        )
        updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
        self.pending_payment_check_channel_ids.add(channel.id)
        confirm_pressed_at_utc = datetime.now(timezone.utc)
        task = asyncio.create_task(
            self.run_payment_confirmation_check(
                channel,
                interaction.user.id,
                confirm_pressed_at_utc=confirm_pressed_at_utc,
            )
        )
        self.payment_check_tasks.add(task)
        task.add_done_callback(self.cleanup_payment_task)
        await self.audit_purchase_event(
            "payment_check_scheduled",
            event_category="payment",
            status="scheduled",
            trigger="button_press",
            channel=channel,
            interaction=interaction,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            product=selected_product,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=TICKET_STAGE_PAYMENT_PENDING,
            details={
                "delay_seconds": PAYMENT_CHECK_DELAY_SECONDS,
                "confirm_pressed_at_utc": confirm_pressed_at_utc.isoformat(),
            },
        )
        await self.audit_stage_transition(
            trigger="button_press",
            channel=channel,
            interaction=interaction,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=TICKET_STAGE_PAYMENT_PENDING,
            product=selected_product,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
            details={
                "delay_seconds": PAYMENT_CHECK_DELAY_SECONDS,
            },
        )
        try:
            await interaction.response.send_message(
                self.build_payment_check_scheduled_message(),
                ephemeral=True,
            )
        except discord.DiscordException:
            self.logger.exception(
                "payment_check_acknowledgement_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                interaction.user.id,
                utc_timestamp(),
            )
            try:
                await channel.send(
                    self.build_payment_check_scheduled_message(),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "payment_check_acknowledgement_fallback_failed channel_id=%s user_id=%s timestamp=%s",
                    channel.id,
                    interaction.user.id,
                    utc_timestamp(),
                )

    async def handle_payment_platform_button(
        self,
        interaction: discord.Interaction,
        platform_key: str,
    ) -> None:
        channel = interaction.channel
        button_custom_id = self.interaction_custom_id(interaction)
        await self.audit_purchase_event(
            "payment_platform_button_pressed",
            event_category="interaction",
            status="success",
            trigger="button_press",
            interaction=interaction,
            button_custom_id=button_custom_id,
            details={"requested_platform_key": platform_key},
        )
        if not isinstance(channel, discord.TextChannel) or not self.is_purchase_ticket_channel(channel):
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="button used outside purchase ticket channel",
                details={"requested_platform_key": platform_key},
            )
            await interaction.response.send_message(
                "This button only works inside a ticket channel.",
                ephemeral=True,
            )
            return

        owner_id = await self.get_authoritative_ticket_owner_id(channel)
        if owner_id is None:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                button_custom_id=button_custom_id,
                failure_reason="ticket owner could not be resolved",
                details={"requested_platform_key": platform_key},
            )
            await interaction.response.send_message(
                "I couldn't verify the ticket owner from saved state. Please open a support ticket from the support panel if this keeps happening.",
                ephemeral=True,
            )
            return

        if interaction.user.id != owner_id:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                button_custom_id=button_custom_id,
                failure_reason="non-owner attempted to choose payment platform",
                details={"requested_platform_key": platform_key},
            )
            await interaction.response.send_message(
                "Only the ticket owner can choose the payment platform here.",
                ephemeral=True,
            )
            return

        selected_platform = get_payment_platform_by_key(platform_key)
        if selected_platform is None:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                button_custom_id=button_custom_id,
                failure_reason="requested payment platform unavailable",
                details={"requested_platform_key": platform_key},
            )
            await interaction.response.send_message(
                "That payment platform isn't available right now.",
                ephemeral=True,
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )

        if ticket_stage == TICKET_STAGE_COMPLETED:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="ignored",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                platform=selected_platform,
                button_custom_id=button_custom_id,
                failure_reason="ticket already completed",
            )
            await interaction.response.send_message(
                "Payment has already been confirmed for this ticket.",
                ephemeral=True,
            )
            return

        if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="ignored",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                platform=selected_platform,
                button_custom_id=button_custom_id,
                failure_reason="payment check already running",
            )
            await interaction.response.send_message(
                self.build_payment_check_running_message(),
                ephemeral=True,
            )
            return

        if selected_product is None or ticket_stage not in {
            TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
            TICKET_STAGE_AWAITING_PAYMENT,
        }:
            await self.audit_purchase_event(
                "payment_platform_rejected",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                platform=selected_platform,
                button_custom_id=button_custom_id,
                failure_reason="script selection not ready for payment platform choice",
            )
            await interaction.response.send_message(
                "Confirm your script selection first before choosing a payment platform.",
                ephemeral=True,
            )
            return

        payment_note_code = await self.ensure_payment_note_code(
            channel.id,
            owner_id=owner_id,
        )
        previous_ticket_stage = ticket_stage

        try:
            await interaction.response.send_message(
                build_payment_instruction_message(
                    selected_product,
                    selected_platform,
                    payment_note_code,
                    ticket_price_override=self.get_ticket_price_override(ticket_record),
                ),
                view=self.build_payment_confirmation_view(),
                allowed_mentions=self.response_allowed_mentions,
            )
        except discord.DiscordException as exc:
            self.logger.exception(
                "ticket_payment_instructions_send_failed channel_id=%s user_id=%s platform=%s timestamp=%s",
                channel.id,
                interaction.user.id,
                selected_platform.key,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "payment_instructions_failed",
                event_category="payment",
                status="failure",
                trigger="button_press",
                channel=channel,
                interaction=interaction,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=selected_product,
                platform=selected_platform,
                payment_note_code=payment_note_code,
                button_custom_id=button_custom_id,
                error=exc,
                failure_reason="payment instructions send failed",
            )
            if not interaction.response.is_done():
                try:
                    await interaction.response.send_message(
                        "I couldn't send the payment instructions right now. Please try again shortly.",
                        ephemeral=True,
                    )
                except discord.DiscordException:
                    pass
            return

        await self.update_ticket_record(
            channel.id,
            owner_id=owner_id,
            payment_platform_key=selected_platform.key,
            payment_note_code=payment_note_code,
            stage=TICKET_STAGE_AWAITING_PAYMENT,
        )
        updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
        self.logger.info(
            "ticket_payment_platform_selected channel_id=%s user_id=%s script=%s platform=%s timestamp=%s",
            channel.id,
            interaction.user.id,
            selected_product.key,
            selected_platform.key,
            utc_timestamp(),
        )
        await self.audit_purchase_event(
            "payment_platform_chosen",
            event_category="payment",
            status="success",
            trigger="button_press",
            channel=channel,
            interaction=interaction,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            product=selected_product,
            platform=selected_platform,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
        )
        await self.audit_purchase_event(
            "payment_instructions_issued",
            event_category="payment",
            status="success",
            trigger="button_press",
            channel=channel,
            interaction=interaction,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            product=selected_product,
            platform=selected_platform,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT,
        )
        await self.audit_stage_transition(
            trigger="button_press",
            channel=channel,
            interaction=interaction,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            previous_ticket_stage=previous_ticket_stage,
            next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT,
            product=selected_product,
            platform=selected_platform,
            payment_note_code=payment_note_code,
            button_custom_id=button_custom_id,
        )

    async def run_payment_confirmation_check(
        self,
        channel: discord.TextChannel,
        user_id: int,
        *,
        confirm_pressed_at_utc: datetime,
    ) -> None:
        self.logger.info(
            "payment_check_scheduled channel_id=%s user_id=%s delay_seconds=%s timestamp=%s",
            channel.id,
            user_id,
            PAYMENT_CHECK_DELAY_SECONDS,
            utc_timestamp(),
        )

        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        pending_ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_PAYMENT_PENDING),
        )
        parser_result: PaymentParserResult | None = None
        message_text = "payment check failed right now"
        await self.audit_purchase_event(
            "payment_check_started",
            event_category="payment",
            status="in_progress",
            trigger="scheduled_task",
            channel=channel,
            actor_user_id=user_id,
            ticket_owner_id=user_id,
            ticket_record=ticket_record,
            product=selected_product,
            payment_note_code=payment_note_code,
            details={
                "delay_seconds": PAYMENT_CHECK_DELAY_SECONDS,
                "confirm_pressed_at_utc": confirm_pressed_at_utc.isoformat(),
            },
        )

        try:
            await asyncio.sleep(PAYMENT_CHECK_DELAY_SECONDS)
            expected_amount = self.get_effective_ticket_price_decimal(
                selected_product,
                ticket_record=ticket_record,
            ) or PAYMENT_PARSER_EXPECTED_AMOUNT
            if not payment_note_code:
                parser_result = {
                    "matched": False,
                    "reason": "payment note code unavailable",
                }
            else:
                async with self.payment_parser_lock:
                    consumed_message_ids = await self.get_consumed_payment_message_ids_snapshot()
                    try:
                        parser_result = await asyncio.wait_for(
                            asyncio.to_thread(
                                check_payment_email,
                                confirm_pressed_at_utc=confirm_pressed_at_utc,
                                expected_amount=expected_amount,
                                expected_payment_note=payment_note_code,
                                consumed_message_ids=consumed_message_ids,
                            ),
                            timeout=PAYMENT_PARSER_TIMEOUT_SECONDS,
                        )
                    except TimeoutError:
                        parser_result = {
                            "matched": False,
                            "reason": "payment parser timed out",
                        }
                    if parser_result.get("matched"):
                        gmail_message_id = cast(
                            str | None,
                            parser_result.get("gmail_message_id"),
                        )
                        if not gmail_message_id:
                            parser_result = {
                                **parser_result,
                                "matched": False,
                                "reason": "parser result missing message id",
                            }
                        else:
                            consumed_ok = await self.record_consumed_payment_message_id(
                                gmail_message_id
                            )
                            if not consumed_ok:
                                parser_result = {
                                    **parser_result,
                                    "matched": False,
                                    "reason": "payment confirmation could not be recorded safely",
                                }
            self.logger.info(
                "payment_check_completed channel_id=%s user_id=%s matched=%s reason=%r gmail_message_id=%s from_address=%r from_domain=%r amount=%r currency=%r received_timestamp_utc=%r auth_summary=%r forwarding_flags=%s timestamp=%s",
                channel.id,
                user_id,
                None if parser_result is None else parser_result.get("matched"),
                None if parser_result is None else parser_result.get("reason"),
                None if parser_result is None else parser_result.get("gmail_message_id"),
                None if parser_result is None else parser_result.get("from_address"),
                None if parser_result is None else parser_result.get("from_domain"),
                None if parser_result is None else parser_result.get("amount"),
                None if parser_result is None else parser_result.get("currency"),
                None if parser_result is None else parser_result.get("received_timestamp_utc"),
                None if parser_result is None else parser_result.get("auth_summary"),
                [] if parser_result is None else parser_result.get("forwarding_flags", []),
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "payment_email_check_result",
                event_category="payment",
                status=(
                    "success"
                    if parser_result is not None and parser_result.get("matched") is True
                    else "failure"
                ),
                trigger="payment_parser",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=ticket_record,
                product=selected_product,
                payment_note_code=payment_note_code,
                gmail_message_id=(
                    cast(str | None, parser_result.get("gmail_message_id"))
                    if parser_result is not None
                    else None
                ),
                failure_reason=(
                    cast(str | None, parser_result.get("reason"))
                    if parser_result is not None and parser_result.get("matched") is False
                    else ""
                ),
                details={
                    "parser_result": parser_result,
                    "confirm_pressed_at_utc": confirm_pressed_at_utc.isoformat(),
                    "expected_amount": str(expected_amount),
                },
            )
        except Exception as exc:
            self.logger.exception(
                "payment_check_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                user_id,
                utc_timestamp(),
            )
            await self.update_ticket_record(
                channel.id,
                stage=TICKET_STAGE_AWAITING_PAYMENT,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            await self.audit_purchase_event(
                "payment_check_failed",
                event_category="exception",
                status="failure",
                trigger="scheduled_task",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=updated_ticket_record,
                product=selected_product,
                payment_note_code=payment_note_code,
                error=exc,
                failure_reason="unexpected exception during payment check",
            )
            await self.audit_stage_transition(
                trigger="scheduled_task",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=updated_ticket_record,
                previous_ticket_stage=pending_ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT,
                product=selected_product,
                payment_note_code=payment_note_code,
                error=exc,
            )
        finally:
            self.pending_payment_check_channel_ids.discard(channel.id)

        if parser_result is not None and parser_result.get("matched") is True:
            await self.audit_purchase_event(
                "payment_verified",
                event_category="payment",
                status="success",
                trigger="payment_parser",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=ticket_record,
                product=selected_product,
                payment_note_code=payment_note_code,
                gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                details={"parser_result": parser_result},
            )
            if selected_product is None:
                await self.update_ticket_record(
                    channel.id,
                    stage=TICKET_STAGE_PAYMENT_PENDING,
                )
                pending_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                message_text = (
                    self.build_support_ticket_manual_review_message(
                        "Payment was confirmed, but I could not determine which script was selected."
                    )
                )
                await self.audit_purchase_event(
                    "support_escalation_triggered",
                    event_category="support",
                    status="failure",
                    trigger="payment_verification",
                    channel=channel,
                    actor_user_id=user_id,
                    ticket_owner_id=user_id,
                    ticket_record=pending_ticket_record,
                    payment_note_code=payment_note_code,
                    gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                    failure_reason="verified payment but selected product was missing",
                    details={"parser_result": parser_result},
                )
            else:
                await self.audit_purchase_event(
                    "file_delivery_attempted",
                    event_category="delivery",
                    status="in_progress",
                    trigger="payment_verification",
                    channel=channel,
                    actor_user_id=user_id,
                    ticket_owner_id=user_id,
                    ticket_record=ticket_record,
                    product=selected_product,
                    payment_note_code=payment_note_code,
                    gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                    delivery_filename=selected_product.file_path.name,
                )
                try:
                    await channel.send(
                        (
                            f"Payment confirmed for {selected_product.label}.\n"
                            f"Here is your `{selected_product.file_path.name}` file.\n"
                            f"What happens next: this purchase ticket will close automatically in {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes.\n"
                            "If you need another script after that, open a new ticket from the panel."
                        ),
                        file=build_script_delivery_file(selected_product),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    auto_close_at_utc = self.build_purchase_ticket_auto_close_deadline()
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_COMPLETED,
                        auto_close_at_utc=auto_close_at_utc,
                    )
                    completed_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                    await self.schedule_purchase_ticket_auto_close(
                        channel,
                        auto_close_at_utc=auto_close_at_utc,
                    )
                    await self.audit_purchase_event(
                        "file_delivery_succeeded",
                        event_category="delivery",
                        status="success",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=completed_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                    )
                    await self.audit_purchase_event(
                        "ticket_marked_completed",
                        event_category="ticket",
                        status="success",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=completed_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        previous_ticket_stage=pending_ticket_stage,
                        next_ticket_stage=TICKET_STAGE_COMPLETED,
                    )
                    await self.audit_stage_transition(
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=completed_ticket_record,
                        previous_ticket_stage=pending_ticket_stage,
                        next_ticket_stage=TICKET_STAGE_COMPLETED,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        details={"parser_result": parser_result},
                    )
                    await self.record_successful_purchase(
                        channel,
                        user_id,
                        selected_product,
                        ticket_record=completed_ticket_record,
                    )
                    return
                except FileNotFoundError as exc:
                    self.logger.exception(
                        "ticket_script_file_missing channel_id=%s user_id=%s script=%s file_path=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        selected_product.file_path,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    pending_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                    message_text = (
                        self.build_support_ticket_manual_review_message(
                            "Payment was confirmed, but the delivery file is missing right now."
                        )
                    )
                    await self.audit_purchase_event(
                        "file_delivery_failed",
                        event_category="delivery",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        error=exc,
                        failure_reason="delivery file missing",
                    )
                    await self.audit_purchase_event(
                        "support_escalation_triggered",
                        event_category="support",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        failure_reason="delivery file missing after verified payment",
                    )
                except (OSError, discord.DiscordException) as exc:
                    self.logger.exception(
                        "ticket_script_delivery_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    pending_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                    message_text = (
                        self.build_support_ticket_manual_review_message(
                            "Payment was confirmed, but I could not send the delivery file right now."
                        )
                    )
                    await self.audit_purchase_event(
                        "file_delivery_failed",
                        event_category="delivery",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        error=exc,
                        failure_reason="delivery send failed",
                    )
                    await self.audit_purchase_event(
                        "support_escalation_triggered",
                        event_category="support",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        failure_reason="delivery send failed after verified payment",
                    )
                except Exception as exc:
                    self.logger.exception(
                        "payment_success_flow_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    pending_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                    message_text = (
                        self.build_support_ticket_manual_review_message(
                            "Payment was confirmed, but I could not finish the delivery flow right now."
                        )
                    )
                    await self.audit_purchase_event(
                        "file_delivery_failed",
                        event_category="delivery",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        error=exc,
                        failure_reason="unexpected exception during delivery flow",
                    )
                    await self.audit_purchase_event(
                        "support_escalation_triggered",
                        event_category="support",
                        status="failure",
                        trigger="payment_verification",
                        channel=channel,
                        actor_user_id=user_id,
                        ticket_owner_id=user_id,
                        ticket_record=pending_ticket_record,
                        product=selected_product,
                        payment_note_code=payment_note_code,
                        gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                        delivery_filename=selected_product.file_path.name,
                        failure_reason="unexpected exception after verified payment",
                    )
        elif parser_result is not None and parser_result.get("matched") is False:
            await self.update_ticket_record(
                channel.id,
                stage=TICKET_STAGE_AWAITING_PAYMENT,
            )
            awaiting_payment_record = await self.get_ticket_record_snapshot(channel.id)
            message_text = self.build_payment_parser_failure_message(parser_result)
            await self.audit_purchase_event(
                "payment_rejected",
                event_category="payment",
                status="failure",
                trigger="payment_parser",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=awaiting_payment_record,
                product=selected_product,
                payment_note_code=payment_note_code,
                gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                failure_reason=cast(str | None, parser_result.get("reason")) or "payment not found",
                details={"parser_result": parser_result},
            )
            await self.audit_stage_transition(
                trigger="payment_parser",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=awaiting_payment_record,
                previous_ticket_stage=pending_ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT,
                product=selected_product,
                payment_note_code=payment_note_code,
                details={"parser_result": parser_result},
            )
            if "support ticket" in message_text.lower() or "contact support" in message_text.lower():
                await self.audit_purchase_event(
                    "support_escalation_triggered",
                    event_category="support",
                    status="failure",
                    trigger="payment_parser",
                    channel=channel,
                    actor_user_id=user_id,
                    ticket_owner_id=user_id,
                    ticket_record=awaiting_payment_record,
                    product=selected_product,
                    payment_note_code=payment_note_code,
                    gmail_message_id=cast(str | None, parser_result.get("gmail_message_id")),
                    failure_reason=cast(str | None, parser_result.get("reason")) or "payment rejected",
                )

        try:
            await channel.send(
                message_text,
                allowed_mentions=self.response_allowed_mentions,
            )
        except discord.DiscordException as exc:
            self.logger.exception(
                "payment_check_message_send_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                user_id,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "payment_check_result_message_failed",
                event_category="exception",
                status="failure",
                trigger="bot_reply",
                channel=channel,
                actor_user_id=user_id,
                ticket_owner_id=user_id,
                ticket_record=await self.get_ticket_record_snapshot(channel.id),
                product=selected_product,
                payment_note_code=payment_note_code,
                error=exc,
                failure_reason="payment result message send failed",
            )

    def ensure_state_save_retry_task(self) -> None:
        if (
            self.state_save_retry_task is not None
            and not self.state_save_retry_task.done()
        ):
            return
        self.state_save_retry_task = asyncio.create_task(self.state_save_retry_loop())

    async def state_save_retry_loop(self) -> None:
        try:
            while not self.is_closed():
                await asyncio.sleep(STATE_SAVE_RETRY_INTERVAL_SECONDS)
                async with self.state_lock:
                    if not self.state_save_failed:
                        return
                    if self.persist_state():
                        self.logger.info(
                            "state_save_recovered source=%s timestamp=%s",
                            self.state_load_source,
                            utc_timestamp(),
                        )
                        return
        except asyncio.CancelledError:
            return

    def persist_state(self) -> bool:
        try:
            save_state(self.state)
            self.state_save_failed = False
            return True
        except OSError:
            if not self.state_save_failed:
                self.logger.exception("state_save_failed timestamp=%s", utc_timestamp())
            self.state_save_failed = True
            self.ensure_state_save_retry_task()
            return False

    async def get_consumed_payment_message_ids_snapshot(self) -> set[str]:
        async with self.state_lock:
            parser_state = get_payment_parser_state(self.state)
            state_changed = purge_consumed_message_ids(parser_state)
            consumed_message_ids = set(parser_state["consumed_message_ids"])
            if state_changed:
                self.persist_state()
            return consumed_message_ids

    async def record_consumed_payment_message_id(self, gmail_message_id: str) -> bool:
        async with self.state_lock:
            parser_state = get_payment_parser_state(self.state)
            purge_consumed_message_ids(parser_state)
            if gmail_message_id in parser_state["consumed_message_ids"]:
                return False

            record_consumed_message_id(parser_state, gmail_message_id)
            if not self.persist_state():
                self.logger.error(
                    "payment_parser_state_save_failed gmail_message_id=%s timestamp=%s",
                    gmail_message_id,
                    utc_timestamp(),
                )
                return False
            return True

    def build_payment_parser_failure_message(
        self,
        parser_result: PaymentParserResult,
    ) -> str:
        reason = cast(str, parser_result.get("reason", "payment not detected yet"))
        from_domain = cast(str | None, parser_result.get("from_domain"))
        allowed_sender_domains = cast(
            list[str] | None,
            parser_result.get("allowed_sender_domains"),
        ) or []
        allowed_sender_subdomains = cast(
            list[str] | None,
            parser_result.get("allowed_sender_subdomains"),
        ) or []
        auth_summary = cast(str | None, parser_result.get("auth_summary"))
        if reason == "no candidate messages found":
            return (
                "Automatic verification did not find your payment in the recent inbox window yet.\n"
                "Do this now: if you just paid, wait a moment and press `Check My Payment` again.\n"
                "What happens next: if the payment has already gone through and it still is not detected, open a support ticket from the support panel for manual review."
            )
        if reason == "payment parser timed out":
            return (
                "Automatic verification is taking longer than expected right now.\n"
                "Do this now: wait a moment and press `Check My Payment` again.\n"
                "What happens next: if this keeps timing out after your payment already went through, open a support ticket from the support panel for manual review."
            )
        if reason == "payment note code unavailable":
            return (
                "This ticket is missing its required payment note code. Choose the payment platform again "
                "and use the exact code shown before checking payment."
            )
        if reason == "payment note missing":
            payment_note = cast(str | None, parser_result.get("expected_payment_note"))
            if payment_note:
                return (
                    f"Automatic verification could not find the required payment code `{payment_note}` in the receipt email.\n"
                    f"Do this now: send the payment with that exact note code `{payment_note}`.\n"
                    "What happens next: if you already paid without that exact code, open a support ticket from the support panel for manual review."
                )
        if reason == "sender domain not allowed":
            allowed_text = ", ".join(allowed_sender_domains) if allowed_sender_domains else "the approved list"
            if from_domain:
                return self.build_support_ticket_manual_review_message(
                    f"Automatic verification could not approve the receipt source `{from_domain}`. Approved sender domains: `{allowed_text}`."
                )
        if reason == "sender subdomain not explicitly approved":
            allowed_text = ", ".join(allowed_sender_subdomains) if allowed_sender_subdomains else "the approved subdomain list"
            if from_domain:
                return self.build_support_ticket_manual_review_message(
                    f"Automatic verification could not approve the receipt source `{from_domain}`. Approved sender subdomains: `{allowed_text}`."
                )
        if reason == "authentication failure" and auth_summary:
            return self.build_support_ticket_manual_review_message(
                f"Automatic verification could not verify the sender authentication ({auth_summary})."
            )
        if reason == "amount short":
            shortfall = cast(str | None, parser_result.get("amount_shortfall"))
            if shortfall:
                return (
                    f"Automatic verification found a payment, but it is ${shortfall} short.\n"
                    f"Do this now: send the remaining ${shortfall} with the correct note code, then press `Check My Payment` again.\n"
                    "What happens next: if you need manual help instead, open a support ticket from the support panel."
                )
        return self.build_support_ticket_manual_review_message(
            f"Automatic verification could not complete successfully ({reason})."
        )

    async def send_response(self, message: discord.Message, text: str) -> None:
        chunks = split_message(text)
        first_chunk = True
        for chunk in chunks:
            try:
                if first_chunk:
                    await message.reply(
                        chunk,
                        mention_author=False,
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    first_chunk = False
                else:
                    await message.channel.send(
                        chunk,
                        allowed_mentions=self.response_allowed_mentions,
                    )
            except discord.DiscordException:
                if first_chunk:
                    try:
                        await message.channel.send(
                            chunk,
                            allowed_mentions=self.response_allowed_mentions,
                        )
                        first_chunk = False
                        continue
                    except discord.DiscordException:
                        pass

                self.logger.exception(
                    "message_response_send_failed channel_id=%s user_id=%s timestamp=%s",
                    message.channel.id,
                    message.author.id,
                    utc_timestamp(),
                )
                return

    async def send_script_selection_confirmation(
        self,
        message: discord.Message,
        product: ScriptProduct,
    ) -> bool:
        channel = message.channel if isinstance(message.channel, discord.TextChannel) else None
        ticket_record = (
            await self.get_ticket_record_snapshot(channel.id)
            if channel is not None
            else None
        )
        try:
            await message.reply(
                build_script_confirmation_message(
                    product,
                    ticket_price_override=self.get_ticket_price_override(ticket_record),
                ),
                mention_author=False,
                allowed_mentions=self.response_allowed_mentions,
            )
            await self.audit_purchase_event(
                "selection_confirmation_prompted",
                event_category="selection",
                status="success",
                trigger="bot_reply",
                channel=channel,
                message=message,
                ticket_record=ticket_record,
                product=product,
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "ticket_selection_confirmation_send_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                message.channel.id,
                message.author.id,
                product.key,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "selection_confirmation_prompt_failed",
                event_category="selection",
                status="failure",
                trigger="bot_reply",
                channel=channel,
                message=message,
                ticket_record=ticket_record,
                product=product,
                error=None,
                failure_reason="discord send failed",
            )
            await self.send_response(
                message,
                "I couldn't send the confirmation message right now. Please try again shortly.",
            )
            return False

    async def send_payment_platform_prompt(self, message: discord.Message) -> bool:
        channel_id = cast(int, message.channel.id)
        ticket_record = await self.get_ticket_record_snapshot(channel_id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        channel = message.channel if isinstance(message.channel, discord.TextChannel) else None
        if selected_product is None:
            await self.audit_purchase_event(
                "payment_platform_prompt_failed",
                event_category="payment",
                status="failure",
                trigger="bot_reply",
                channel=channel,
                message=message,
                ticket_record=ticket_record,
                failure_reason="selected product missing",
            )
            await self.send_response(
                message,
                "I couldn't determine which script you selected. Please choose the script again.",
            )
            return False

        try:
            await message.reply(
                build_payment_platform_prompt_message(
                    selected_product,
                    ticket_price_override=self.get_ticket_price_override(ticket_record),
                ),
                mention_author=False,
                view=self.build_payment_platform_selection_view(),
                allowed_mentions=self.response_allowed_mentions,
            )
            await self.audit_purchase_event(
                "payment_platform_prompted",
                event_category="payment",
                status="success",
                trigger="bot_reply",
                channel=channel,
                message=message,
                ticket_record=ticket_record,
                product=selected_product,
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "ticket_payment_platform_prompt_send_failed channel_id=%s user_id=%s timestamp=%s",
                message.channel.id,
                message.author.id,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "payment_platform_prompt_failed",
                event_category="payment",
                status="failure",
                trigger="bot_reply",
                channel=channel,
                message=message,
                ticket_record=ticket_record,
                product=selected_product,
                failure_reason="discord send failed",
            )
            await self.send_response(
                message,
                "I couldn't send the payment platform options right now. Please try again shortly.",
            )
            return False

    async def build_purchase_record(
        self,
        channel: discord.TextChannel,
        user_id: int,
        product: ScriptProduct,
        *,
        payment_platform: PaymentPlatform | None = None,
        price_paid: str,
        purchase_timestamp: str,
    ) -> PurchaseRecord:
        username, display_name = await self.resolve_user_identity(
            user_id,
            guild=channel.guild,
        )
        guild_id = channel.guild.id if channel.guild is not None else 0
        full_date = datetime.fromisoformat(purchase_timestamp).date().isoformat()
        return {
            "Full Date": full_date,
            "Exact Timestamp": purchase_timestamp,
            "Discord Username": username,
            "Display Name": display_name or username,
            "User ID": user_id,
            "Item Purchased": product.label,
            "Item Key": product.key,
            "Delivered File": product.file_path.name,
            "Price Paid": price_paid,
            "Payment Method": "" if payment_platform is None else payment_platform.label,
            "Payment Method Key": "" if payment_platform is None else payment_platform.key,
            "Channel ID": channel.id,
            "Guild ID": guild_id,
            "Purchase Event ID": uuid4().hex,
        }

    async def record_successful_purchase(
        self,
        channel: discord.TextChannel,
        user_id: int,
        product: ScriptProduct,
        *,
        ticket_record: TicketRecord | None = None,
    ) -> None:
        purchase_timestamp = utc_timestamp()
        price_paid = self.get_effective_ticket_price_text(
            product,
            ticket_record=ticket_record,
        ) or (resolve_ticket_price_text(product) or "0.00")
        payment_platform = None
        if ticket_record is not None:
            payment_platform = get_payment_platform_by_key(
                cast(str | None, ticket_record.get("payment_platform_key"))
            )
        purchase_record = await self.build_purchase_record(
            channel,
            user_id,
            product,
            payment_platform=payment_platform,
            price_paid=price_paid,
            purchase_timestamp=purchase_timestamp,
        )
        async with self.purchase_sync_lock:
            try:
                queued_ok, local_ok, sheet_ok = await asyncio.to_thread(
                    self.purchase_logger.queue_and_sync_purchase,
                    purchase_record,
                )
            except Exception:
                self.logger.exception(
                    "purchase_log_record_unexpected_failure channel_id=%s user_id=%s item_key=%s purchase_event_id=%s timestamp=%s",
                    channel.id,
                    user_id,
                    product.key,
                    purchase_record["Purchase Event ID"],
                    purchase_timestamp,
                )
                await self.audit_purchase_event(
                    "purchase_record_processed",
                    event_category="purchase_log",
                    status="failure",
                    trigger="post_delivery",
                    channel=channel,
                    actor_user_id=user_id,
                    ticket_owner_id=user_id,
                    ticket_stage=TICKET_STAGE_COMPLETED,
                    product=product,
                    purchase_event_id=purchase_record["Purchase Event ID"],
                    failure_reason="purchase logger raised an unexpected exception",
                    details={
                        "recovery_path": str(self.purchase_logger.recovery_file),
                    },
                )
                return

        log_message = (
            "purchase_log_record_processed "
            "channel_id=%s user_id=%s item_key=%s purchase_event_id=%s "
            "timestamp=%s queued_ok=%s local_ok=%s sheet_ok=%s recovery_path=%s"
        )
        if not queued_ok:
            self.logger.error(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )
        elif not (local_ok and sheet_ok):
            self.logger.warning(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )
        else:
            self.logger.info(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )
        await self.audit_purchase_event(
            "purchase_record_processed",
            event_category="purchase_log",
            status=(
                "failure"
                if not queued_ok
                else "warning"
                if not (local_ok and sheet_ok)
                else "success"
            ),
            trigger="post_delivery",
            channel=channel,
            actor_user_id=user_id,
            ticket_owner_id=user_id,
            ticket_stage=TICKET_STAGE_COMPLETED,
            product=product,
            purchase_event_id=purchase_record["Purchase Event ID"],
            failure_reason=(
                "recovery queue append failed"
                if not queued_ok
                else "purchase log sync incomplete"
                if not (local_ok and sheet_ok)
                else ""
            ),
            details={
                "queued_ok": queued_ok,
                "local_ok": local_ok,
                "sheet_ok": sheet_ok,
                "recovery_path": str(self.purchase_logger.recovery_file),
                "purchase_log_path": str(self.purchase_logger.purchase_log_file),
            },
        )

    def resolve_admin_stage_input(self, raw_stage: str) -> str | None:
        stripped_stage = raw_stage.strip()
        normalized_stage = normalize_text(stripped_stage)
        for candidate in VALID_TICKET_STAGES:
            if stripped_stage == candidate or normalize_text(candidate) == normalized_stage:
                return candidate
        return None

    async def get_admin_purchase_ticket_context(
        self,
        message: discord.Message,
    ) -> (
        tuple[
            discord.TextChannel,
            int,
            TicketRecord,
            str,
            ScriptProduct | None,
            PaymentPlatform | None,
            str | None,
        ]
        | None
    ):
        channel = message.channel if isinstance(message.channel, discord.TextChannel) else None
        if channel is None or not self.is_purchase_ticket_channel(channel):
            await self.audit_admin_event(
                "admin_command_rejected",
                status="failure",
                message=message,
                channel=channel,
                failure_reason="admin command requires purchase ticket channel",
            )
            await self.send_response(
                message,
                "This admin test command only works inside a purchase ticket channel.",
            )
            return None

        owner_id = await self.get_authoritative_ticket_owner_id(channel)
        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        if owner_id is None:
            await self.audit_admin_event(
                "admin_command_rejected",
                status="failure",
                message=message,
                channel=channel,
                ticket_record=ticket_record,
                failure_reason="ticket owner could not be resolved for admin command",
            )
            await self.send_response(
                message,
                "I couldn't resolve the ticket owner for this channel.",
            )
            return None

        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )
        current_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        current_platform = get_payment_platform_by_key(
            cast(str | None, ticket_record.get("payment_platform_key"))
        )
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        return (
            channel,
            owner_id,
            ticket_record,
            ticket_stage,
            current_product,
            current_platform,
            payment_note_code,
        )

    async def send_admin_bypass_delivery(
        self,
        *,
        message: discord.Message,
        channel: discord.TextChannel,
        ticket_owner_id: int,
        ticket_record: TicketRecord,
        product: ScriptProduct,
        payment_note_code: str | None,
        previous_ticket_stage: str,
        mark_completed: bool,
        admin_action: str,
    ) -> bool:
        details = {
            "admin_action": admin_action,
            "processed_via": "admin_bypass",
        }
        await self.audit_purchase_event(
            "file_delivery_attempted",
            event_category="delivery",
            status="in_progress",
            trigger=ADMIN_COMMAND_TRIGGER,
            channel=channel,
            message=message,
            ticket_owner_id=ticket_owner_id,
            ticket_record=ticket_record,
            product=product,
            payment_note_code=payment_note_code,
            delivery_filename=product.file_path.name,
            details=details,
        )

        try:
            await channel.send(
                (
                    "Admin bypass for testing: sending "
                    f"`{product.file_path.name}` for {product.label}. "
                    "This was processed via admin bypass."
                    + (
                        f"\n\nThis completed purchase ticket will close automatically in {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes."
                        if mark_completed
                        else ""
                    )
                ),
                file=build_script_delivery_file(product),
                allowed_mentions=self.response_allowed_mentions,
            )
        except FileNotFoundError as exc:
            self.logger.exception(
                "admin_bypass_delivery_file_missing channel_id=%s admin_user_id=%s ticket_owner_id=%s script=%s timestamp=%s",
                channel.id,
                message.author.id,
                ticket_owner_id,
                product.key,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "file_delivery_failed",
                event_category="delivery",
                status="failure",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=ticket_owner_id,
                ticket_record=ticket_record,
                product=product,
                payment_note_code=payment_note_code,
                delivery_filename=product.file_path.name,
                error=exc,
                failure_reason="delivery file missing during admin bypass",
                details=details,
            )
            await self.send_response(
                message,
                (
                    f"Admin bypass failed because `{product.file_path.name}` is missing "
                    "from the asset directory."
                ),
            )
            return False
        except (OSError, discord.DiscordException) as exc:
            self.logger.exception(
                "admin_bypass_delivery_failed channel_id=%s admin_user_id=%s ticket_owner_id=%s script=%s timestamp=%s",
                channel.id,
                message.author.id,
                ticket_owner_id,
                product.key,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "file_delivery_failed",
                event_category="delivery",
                status="failure",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=ticket_owner_id,
                ticket_record=ticket_record,
                product=product,
                payment_note_code=payment_note_code,
                delivery_filename=product.file_path.name,
                error=exc,
                failure_reason="delivery send failed during admin bypass",
                details=details,
            )
            await self.send_response(
                message,
                "Admin bypass delivery failed while sending the file to Discord.",
            )
            return False

        updated_ticket_record = ticket_record
        if mark_completed:
            auto_close_at_utc = self.build_purchase_ticket_auto_close_deadline()
            await self.update_ticket_record(
                channel.id,
                owner_id=ticket_owner_id,
                selected_script_key=product.key,
                stage=TICKET_STAGE_COMPLETED,
                auto_close_at_utc=auto_close_at_utc,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            await self.schedule_purchase_ticket_auto_close(
                channel,
                auto_close_at_utc=auto_close_at_utc,
            )

        await self.audit_purchase_event(
            "file_delivery_succeeded",
            event_category="delivery",
            status="success",
            trigger=ADMIN_COMMAND_TRIGGER,
            channel=channel,
            message=message,
            ticket_owner_id=ticket_owner_id,
            ticket_record=updated_ticket_record,
            product=product,
            payment_note_code=payment_note_code,
            delivery_filename=product.file_path.name,
            details=details,
        )

        if mark_completed:
            await self.audit_purchase_event(
                "ticket_marked_completed",
                event_category="ticket",
                status="success",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=ticket_owner_id,
                ticket_record=updated_ticket_record,
                product=product,
                payment_note_code=payment_note_code,
                previous_ticket_stage=previous_ticket_stage,
                next_ticket_stage=TICKET_STAGE_COMPLETED,
                details=details,
            )
            await self.audit_stage_transition(
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=ticket_owner_id,
                ticket_record=updated_ticket_record,
                previous_ticket_stage=previous_ticket_stage,
                next_ticket_stage=TICKET_STAGE_COMPLETED,
                product=product,
                payment_note_code=payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                details=details,
            )

        return True

    async def handle_admin_command(self, message: discord.Message) -> bool:
        raw_command = message.content.strip()
        lower_command = raw_command.lower()
        if not (
            lower_command.startswith("!admin")
            or lower_command in ADMIN_DELETE_TICKET_COMMAND_ALIASES
        ):
            return False

        channel = message.channel if isinstance(message.channel, discord.TextChannel) else None
        if not self.is_admin_bypass_user(message.author):
            await self.audit_purchase_event(
                "admin_command_rejected",
                event_category="admin",
                status="failure",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                failure_reason="unauthorized admin command attempt",
                details={
                    "required_username": ADMIN_BYPASS_USERNAME,
                    "required_display_name": ADMIN_BYPASS_DISPLAY_NAME,
                },
            )
            await self.send_response(
                message,
                "You do not have access to the admin bypass test commands.",
            )
            return True

        if lower_command in ADMIN_COMMAND_LIST_ALIASES:
            await self.audit_admin_event(
                "admin_command_listed",
                status="success",
                message=message,
                channel=channel,
            )
            await self.send_response(message, self.build_admin_command_panel_message())
            return True

        if lower_command in ADMIN_CATALOG_COMMAND_ALIASES:
            await self.audit_admin_event(
                "admin_catalog_shown",
                status="success",
                message=message,
                channel=channel,
            )
            await self.send_response(
                message,
                "Current full asset-backed script catalog:\n"
                f"{build_ticket_catalog_lines()}",
            )
            return True

        if lower_command in ADMIN_STATUS_COMMAND_ALIASES:
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                current_product,
                current_platform,
                current_payment_note_code,
            ) = context
            await self.audit_admin_event(
                "admin_status_shown",
                status="success",
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                ticket_stage=ticket_stage,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
            )
            owner_username, owner_display_name = await self.resolve_user_identity(
                owner_id,
                guild=channel.guild,
            )
            price_status_lines = self.build_ticket_price_status_lines(
                current_product,
                ticket_record=ticket_record,
            )
            await self.send_response(
                message,
                (
                    "Admin ticket status\n"
                    f"Owner: {owner_display_name} ({owner_username}, {owner_id})\n"
                    f"Stage: {ticket_stage}\n"
                    f"Selected script: {current_product.label if current_product is not None else 'none'}\n"
                    f"Selected file: {current_product.file_path.name if current_product is not None else 'none'}\n"
                    f"Payment platform: {current_platform.label if current_platform is not None else 'none'}\n"
                    f"Payment note code: {current_payment_note_code or 'none'}\n"
                    + "\n".join(price_status_lines)
                ),
            )
            return True

        if lower_command in ADMIN_VERSION_COMMAND_ALIASES:
            version_message = self.build_admin_version_message()
            await self.audit_admin_event(
                "admin_version_shown",
                status="success",
                message=message,
                channel=channel,
                details={"version_message": version_message},
            )
            await self.send_response(message, version_message)
            return True

        if lower_command in ADMIN_DELETE_TICKET_COMMAND_ALIASES:
            if channel is None:
                await self.audit_admin_event(
                    "admin_ticket_delete_rejected",
                    status="failure",
                    message=message,
                    channel=channel,
                    failure_reason="admin delete requires text channel",
                )
                await self.send_response(
                    message,
                    "This admin delete command only works inside a purchase or support ticket channel.",
                )
                return True

            if self.is_purchase_ticket_channel(channel):
                owner_id = await self.get_authoritative_ticket_owner_id(channel)
                ticket_record = await self.get_ticket_record_snapshot(
                    channel.id,
                    owner_id=owner_id,
                )
                ticket_stage = cast(
                    str,
                    ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
                )
                current_product = get_script_product_by_key(
                    cast(str | None, ticket_record.get("selected_script_key"))
                )
                current_platform = get_payment_platform_by_key(
                    cast(str | None, ticket_record.get("payment_platform_key"))
                )
                current_payment_note_code = cast(
                    str | None,
                    ticket_record.get("payment_note_code"),
                )

                if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                    await self.audit_admin_event(
                        "admin_ticket_delete_rejected",
                        status="failure",
                        message=message,
                        channel=channel,
                        ticket_owner_id=owner_id,
                        ticket_record=ticket_record,
                        ticket_stage=ticket_stage,
                        product=current_product,
                        platform=current_platform,
                        payment_note_code=current_payment_note_code,
                        failure_reason="admin delete requested while payment check was running",
                    )
                    await self.send_response(
                        message,
                        "A payment check is already running for this ticket. Please wait for the result message before using `!D`.",
                    )
                    return True

                await self.audit_admin_event(
                    "admin_ticket_delete_requested",
                    status="success",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                )
                closed_ok = await self.close_purchase_ticket_channel(
                    channel,
                    delete_reason=(
                        f"Purchase ticket deleted by admin command from {message.author} ({message.author.id})"
                    ),
                    closing_message=(
                        "Admin delete requested. This purchase ticket will be deleted in a few seconds."
                    ),
                    grace_period_seconds=3,
                )
                await self.audit_admin_event(
                    "admin_ticket_deleted",
                    status="success" if closed_ok else "failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    failure_reason="" if closed_ok else "purchase ticket delete failed",
                )
                await self.audit_purchase_event(
                    "purchase_ticket_closed",
                    event_category="ticket",
                    status="success" if closed_ok else "failure",
                    trigger=ADMIN_COMMAND_TRIGGER,
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    raw_user_input=message.content,
                    normalized_user_input=normalize_text(message.content),
                    failure_reason="" if closed_ok else "purchase ticket close failed from admin delete",
                )
                if not closed_ok:
                    await self.send_response(
                        message,
                        "I couldn't delete this purchase ticket right now. Please try again.",
                    )
                return True

            if self.is_support_ticket_channel(channel):
                owner_id = ticket_owner_id_from_topic(channel.topic)
                if owner_id is None:
                    owner_id = self.infer_ticket_owner_id_from_overwrites(channel)

                await self.audit_admin_event(
                    "admin_ticket_delete_requested",
                    status="success",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                )
                closed_ok = await self.close_support_ticket_channel(
                    channel,
                    delete_reason=(
                        f"Support ticket deleted by admin command from {message.author} ({message.author.id})"
                    ),
                    closing_message=(
                        "Admin delete requested. This support ticket will be deleted in a few seconds."
                    ),
                    grace_period_seconds=3,
                )
                await self.audit_admin_event(
                    "admin_ticket_deleted",
                    status="success" if closed_ok else "failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    failure_reason="" if closed_ok else "support ticket delete failed",
                )
                await self.audit_purchase_event(
                    "support_ticket_closed",
                    event_category="support",
                    status="success" if closed_ok else "failure",
                    trigger=ADMIN_COMMAND_TRIGGER,
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    raw_user_input=message.content,
                    normalized_user_input=normalize_text(message.content),
                    failure_reason="" if closed_ok else "support ticket close failed from admin delete",
                )
                if not closed_ok:
                    await self.send_response(
                        message,
                        "I couldn't delete this support ticket right now. Please try again.",
                    )
                return True

            await self.audit_admin_event(
                "admin_ticket_delete_rejected",
                status="failure",
                message=message,
                channel=channel,
                failure_reason="admin delete requires ticket channel",
            )
            await self.send_response(
                message,
                "This admin delete command only works inside a purchase or support ticket channel.",
            )
            return True

        selection = self.admin_command_argument(
            raw_command,
            lower_command,
            ADMIN_SET_SCRIPT_COMMAND_PREFIXES,
        )
        if selection is not None:
            if not selection:
                await self.send_response(
                    message,
                    "Usage: `!admin script <name|number|filename|alias>`",
                )
                return True
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                _current_product,
                _current_platform,
                _current_payment_note_code,
            ) = context
            selection_result = resolve_script_product_selection(selection)
            selected_product = selection_result.product
            if selected_product is None:
                await self.audit_admin_event(
                    "admin_script_set_failed",
                    status="failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    failure_reason=selection_result.status,
                    details={"candidate_keys": selection_result.candidate_keys},
                )
                await self.send_response(message, build_ticket_retry_message())
                return True

            await self.update_ticket_record(
                channel.id,
                owner_id=owner_id,
                selected_script_key=selected_product.key,
                payment_platform_key=None,
                payment_note_code=None,
                stage=TICKET_STAGE_AWAITING_CONFIRMATION,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            await self.audit_admin_event(
                "admin_script_set",
                status="success",
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                ticket_stage=TICKET_STAGE_AWAITING_CONFIRMATION,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_CONFIRMATION,
                product=selected_product,
                details={"selection_input": selection},
            )
            await self.audit_purchase_event(
                "product_selection_resolved",
                event_category="selection",
                status="success",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                product=selected_product,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_CONFIRMATION,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                details={"selection_input": selection},
            )
            await self.audit_stage_transition(
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_CONFIRMATION,
                product=selected_product,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                details={"selection_input": selection},
            )
            await self.send_response(
                message,
                "Admin bypass selected this script for testing:\n"
                + build_script_confirmation_message(
                    selected_product,
                    ticket_price_override=self.get_ticket_price_override(
                        updated_ticket_record
                    ),
                ),
            )
            return True

        raw_price_input = self.admin_command_argument(
            raw_command,
            lower_command,
            ADMIN_SET_PRICE_COMMAND_PREFIXES,
        )
        if raw_price_input is not None:
            if not raw_price_input:
                await self.send_response(
                    message,
                    "Usage: `!admin price <amount>` or `!admin price default`",
                )
                return True
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                current_product,
                current_platform,
                current_payment_note_code,
            ) = context

            if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                await self.audit_admin_event(
                    "admin_price_set_failed",
                    status="failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    failure_reason="price override change requested while payment check was running",
                )
                await self.send_response(
                    message,
                    "A payment check is already running for this ticket, so the price can't be changed right now.",
                )
                return True

            if ticket_stage == TICKET_STAGE_COMPLETED:
                await self.audit_admin_event(
                    "admin_price_set_failed",
                    status="failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    failure_reason="price override change requested after ticket completion",
                )
                await self.send_response(
                    message,
                    "This ticket is already completed, so the price override can't be changed now.",
                )
                return True

            normalized_keyword = normalize_text(raw_price_input)
            clear_price_override = normalized_keyword in {
                "default",
                "standard",
                "full",
                "full price",
                "clear",
                "reset",
            }

            ticket_price_override: str | None
            admin_event_type: str
            purchase_event_type: str
            if clear_price_override:
                ticket_price_override = None
                admin_event_type = "admin_price_override_cleared"
                purchase_event_type = "ticket_price_override_cleared"
            else:
                ticket_price_override = normalize_ticket_price_text(raw_price_input)
                if ticket_price_override is None:
                    await self.audit_admin_event(
                        "admin_price_set_failed",
                        status="failure",
                        message=message,
                        channel=channel,
                        ticket_owner_id=owner_id,
                        ticket_record=ticket_record,
                        ticket_stage=ticket_stage,
                        product=current_product,
                        platform=current_platform,
                        payment_note_code=current_payment_note_code,
                        failure_reason="invalid price override",
                        details={"requested_price": raw_price_input},
                    )
                    await self.send_response(
                        message,
                        "Invalid price. Use a positive amount like `15`, `15.00`, or `$15.00`, or use `!admin price default`.",
                    )
                    return True
                admin_event_type = "admin_price_override_set"
                purchase_event_type = "ticket_price_override_set"

            await self.update_ticket_record(
                channel.id,
                owner_id=owner_id,
                ticket_price_override=ticket_price_override,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            effective_price = self.get_effective_ticket_price_text(
                current_product,
                ticket_record=updated_ticket_record,
            )
            standard_price = (
                resolve_ticket_price_text(current_product)
                if current_product is not None
                else None
            )

            await self.audit_admin_event(
                admin_event_type,
                status="success",
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                ticket_stage=ticket_stage,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                details={
                    "requested_price": raw_price_input,
                    "ticket_price_override": ticket_price_override,
                    "effective_price": effective_price,
                    "standard_price": standard_price,
                },
            )
            await self.audit_purchase_event(
                purchase_event_type,
                event_category="payment",
                status="success",
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                ticket_stage=ticket_stage,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                details={
                    "requested_price": raw_price_input,
                    "ticket_price_override": ticket_price_override,
                    "effective_price": effective_price,
                    "standard_price": standard_price,
                },
            )

            response_lines = [
                (
                    "Admin bypass cleared the ticket-specific price override."
                    if clear_price_override
                    else f"Admin bypass set the ticket-specific price to ${ticket_price_override}."
                )
            ]
            if current_product is not None:
                response_lines.append(f"Selected script: {current_product.label}")
                if standard_price is not None:
                    response_lines.append(f"Standard price: ${standard_price}")
                if effective_price is not None:
                    response_lines.append(f"Effective price: ${effective_price}")
            else:
                response_lines.append(
                    "No script is selected yet, so this price will apply once the customer chooses one."
                )
            if (
                current_platform is not None
                and current_payment_note_code
                and effective_price is not None
                and ticket_stage == TICKET_STAGE_AWAITING_PAYMENT
            ):
                response_lines.append(
                    f"Current payment instructions: {current_platform.label}, pay ${effective_price}, note code `{current_payment_note_code}`."
                )
            await self.send_response(message, "\n".join(response_lines))
            return True

        raw_stage = self.admin_command_argument(
            raw_command,
            lower_command,
            ADMIN_SET_STAGE_COMMAND_PREFIXES,
        )
        if raw_stage is not None:
            if not raw_stage:
                await self.send_response(
                    message,
                    "Usage: `!admin stage <stage>`\n"
                    f"Valid stages: {', '.join(sorted(VALID_TICKET_STAGES))}",
                )
                return True
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                current_product,
                current_platform,
                current_payment_note_code,
            ) = context
            resolved_stage = self.resolve_admin_stage_input(raw_stage)
            if resolved_stage is None:
                await self.audit_admin_event(
                    "admin_stage_set_failed",
                    status="failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    failure_reason="invalid stage",
                    details={"requested_stage": raw_stage},
                )
                await self.send_response(
                    message,
                    "Unknown stage. Valid stages are: "
                    f"{', '.join(sorted(VALID_TICKET_STAGES))}",
                )
                return True

            await self.update_ticket_record(
                channel.id,
                owner_id=owner_id,
                stage=resolved_stage,
                auto_close_at_utc=(
                    self.build_purchase_ticket_auto_close_deadline()
                    if resolved_stage == TICKET_STAGE_COMPLETED
                    else None
                ),
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            if resolved_stage == TICKET_STAGE_COMPLETED:
                auto_close_at_utc = cast(
                    str | None,
                    updated_ticket_record.get("auto_close_at_utc"),
                )
                await self.schedule_purchase_ticket_auto_close(
                    channel,
                    auto_close_at_utc=auto_close_at_utc,
                )
            await self.audit_admin_event(
                "admin_stage_set",
                status="success",
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                ticket_stage=resolved_stage,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=resolved_stage,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                details={"requested_stage": raw_stage},
            )
            await self.audit_stage_transition(
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=resolved_stage,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                details={"requested_stage": raw_stage},
            )
            await self.send_response(
                message,
                f"Admin bypass set the ticket stage to `{resolved_stage}`.",
            )
            return True

        if lower_command in ADMIN_RESET_COMMAND_ALIASES:
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                current_product,
                current_platform,
                current_payment_note_code,
            ) = context
            await self.update_ticket_record(
                channel.id,
                owner_id=owner_id,
                selected_script_key=None,
                payment_platform_key=None,
                payment_note_code=None,
                stage=TICKET_STAGE_AWAITING_SELECTION,
            )
            reset_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            await self.audit_admin_event(
                "admin_ticket_reset",
                status="success",
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=reset_ticket_record,
                ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
            )
            await self.audit_stage_transition(
                trigger=ADMIN_COMMAND_TRIGGER,
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=reset_ticket_record,
                previous_ticket_stage=ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
            )
            await self.send_response(
                message,
                "Admin bypass reset the ticket to script selection.",
            )
            return True

        for command_prefixes, canonical_command, mark_completed in (
            (ADMIN_DELIVER_COMMAND_PREFIXES, "!admin deliver", False),
            (ADMIN_BYPASS_COMMAND_PREFIXES, "!admin bypass", True),
        ):
            selection = self.admin_command_argument(
                raw_command,
                lower_command,
                command_prefixes,
            )
            if selection is None:
                continue
            context = await self.get_admin_purchase_ticket_context(message)
            if context is None:
                return True
            (
                channel,
                owner_id,
                ticket_record,
                ticket_stage,
                current_product,
                current_platform,
                current_payment_note_code,
            ) = context

            target_product = current_product
            selection_details: dict[str, object] = {}
            if selection:
                selection_result = resolve_script_product_selection(selection)
                target_product = selection_result.product
                selection_details["selection_input"] = selection
                selection_details["candidate_keys"] = selection_result.candidate_keys
                if target_product is None:
                    await self.audit_admin_event(
                        "admin_delivery_failed",
                        status="failure",
                        message=message,
                        channel=channel,
                        ticket_owner_id=owner_id,
                        ticket_record=ticket_record,
                        ticket_stage=ticket_stage,
                        failure_reason=selection_result.status,
                        details=selection_details,
                    )
                    await self.send_response(message, build_ticket_retry_message())
                    return True

            if target_product is None:
                await self.audit_admin_event(
                    "admin_delivery_failed",
                    status="failure",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    failure_reason="no script selected for admin delivery",
                )
                await self.send_response(
                    message,
                    "No script is currently selected. Use `!admin set-script <script>` or provide a script to the delivery command.",
                )
                return True

            if mark_completed:
                await self.audit_admin_event(
                    "admin_bypass_payment_requested",
                    status="success",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=target_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    details=selection_details,
                )
                await self.audit_purchase_event(
                    "payment_verified",
                    event_category="payment",
                    status="success",
                    trigger=ADMIN_COMMAND_TRIGGER,
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=target_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    details={
                        **selection_details,
                        "verification_mode": "email_check_bypassed",
                    },
                )
            else:
                await self.audit_admin_event(
                    "admin_delivery_requested",
                    status="success",
                    message=message,
                    channel=channel,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    ticket_stage=ticket_stage,
                    product=target_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    details=selection_details,
                )

            delivered_ok = await self.send_admin_bypass_delivery(
                message=message,
                channel=channel,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=target_product,
                payment_note_code=current_payment_note_code,
                previous_ticket_stage=ticket_stage,
                mark_completed=mark_completed,
                admin_action=canonical_command,
            )
            if delivered_ok:
                success_text = (
                    "Admin bypass skipped the email check, delivered the file, and marked the ticket completed."
                    if mark_completed
                    else "Admin bypass delivered the file for testing without changing the purchase state."
                )
                await self.send_response(message, success_text)
            return True

        await self.audit_admin_event(
            "admin_command_invalid",
            status="failure",
            message=message,
            channel=channel,
            failure_reason="unknown admin command",
        )
        await self.send_response(
            message,
            "Unknown admin command.\n"
            f"Use `{ADMIN_COMMAND_LIST}` to view the admin command menu.",
        )
        return True

    async def handle_ticket_prompt(self, message: discord.Message) -> None:
        channel = message.channel
        if not isinstance(channel, discord.TextChannel):
            return

        normalized_input = normalize_text(message.content)
        owner_id = await self.get_authoritative_ticket_owner_id(
            channel,
        )
        if owner_id is None:
            self.logger.warning(
                "ticket_owner_unresolved channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                message.author.id,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "ticket_owner_unresolved",
                event_category="ticket",
                status="failure",
                trigger="user_message",
                channel=channel,
                message=message,
                failure_reason="ticket owner could not be resolved",
            )
            await self.send_response(
                message,
                "I couldn't verify the ticket owner for this channel. Please open a support ticket from the support panel if you still need help.",
            )
            return
        if owner_id is not None and message.author.id != owner_id:
            self.logger.info(
                "ticket_message_ignored_non_owner channel_id=%s user_id=%s owner_id=%s timestamp=%s",
                channel.id,
                message.author.id,
                owner_id,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "ticket_message_ignored_non_owner",
                event_category="ticket",
                status="ignored",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
                failure_reason="message author is not the ticket owner",
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(
            channel.id,
        )
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )
        current_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        current_platform = get_payment_platform_by_key(
            cast(str | None, ticket_record.get("payment_platform_key"))
        )
        current_payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        if current_platform is not None and not current_payment_note_code:
            current_payment_note_code = await self.ensure_payment_note_code(
                channel.id,
                owner_id=message.author.id,
            )

        if message_requests_ticket_close(message.content):
            if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                await self.audit_purchase_event(
                    "purchase_ticket_close_rejected",
                    event_category="ticket",
                    status="failure",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="purchase ticket close requested while payment check was running",
                )
                await self.send_response(
                    message,
                    "A payment check is already running for this ticket, so it can't be closed right now. Please wait for the result message first.",
                )
                return

            await self.audit_purchase_event(
                "purchase_ticket_close_requested",
                event_category="ticket",
                status="success",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
            )
            closed_ok = await self.close_purchase_ticket_channel(
                channel,
                delete_reason=(
                    f"Purchase ticket closed by {message.author} ({message.author.id})"
                ),
                closing_message=(
                    "Closing this purchase ticket now. This channel will be deleted in a few seconds.\n"
                    "If you need another script later, open a new ticket from the panel."
                ),
                grace_period_seconds=3,
            )
            await self.audit_purchase_event(
                "purchase_ticket_closed",
                event_category="ticket",
                status="success" if closed_ok else "failure",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
                failure_reason="" if closed_ok else "purchase ticket close failed",
            )
            if not closed_ok:
                await self.send_response(
                    message,
                    "I couldn't close this purchase ticket right now. Please try again, or open a support ticket from the support panel if you still need help.",
                )
            return

        if message_requests_script_change(message.content):
            if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                await self.audit_purchase_event(
                    "script_change_rejected",
                    event_category="selection",
                    status="failure",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=current_product,
                    platform=current_platform,
                    payment_note_code=current_payment_note_code,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="script change requested while payment check was running",
                )
                await self.send_response(
                    message,
                    "A payment check is already running for this ticket, so the script can't be changed right now. Please wait for the result message first.",
                )
                return

            if ticket_stage == TICKET_STAGE_COMPLETED:
                await self.audit_purchase_event(
                    "script_change_rejected",
                    event_category="selection",
                    status="ignored",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=current_product,
                    payment_note_code=current_payment_note_code,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="script change requested after ticket was completed",
                )
                await self.send_response(
                    message,
                    (
                        "This ticket is already completed.\n"
                        f"What happens next: it will close automatically {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes after delivery.\n"
                        "If you need another script sooner, type `close ticket` and open a new one."
                    ),
                )
                return

            previous_ticket_stage = ticket_stage
            await self.update_ticket_record(
                channel.id,
                owner_id=message.author.id,
                selected_script_key=None,
                payment_platform_key=None,
                payment_note_code=None,
                stage=TICKET_STAGE_AWAITING_SELECTION,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            await self.audit_purchase_event(
                "script_change_requested",
                event_category="selection",
                status="success",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
                previous_ticket_stage=previous_ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
            )
            await self.audit_stage_transition(
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                previous_ticket_stage=previous_ticket_stage,
                next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
            )
            await self.send_response(
                message,
                build_ticket_change_script_message(),
            )
            return

        if ticket_stage == TICKET_STAGE_AWAITING_CONFIRMATION:
            if message_is_selection_confirmation(message.content):
                if current_product is None:
                    await self.audit_purchase_event(
                        "selection_confirm_failed",
                        event_category="selection",
                        status="failure",
                        trigger="user_message",
                        channel=channel,
                        message=message,
                        ticket_owner_id=owner_id,
                        ticket_record=ticket_record,
                        raw_user_input=message.content,
                        normalized_user_input=normalized_input,
                        failure_reason="selected product missing during confirmation",
                    )
                    await self.update_ticket_record(
                        channel.id,
                        owner_id=message.author.id,
                        selected_script_key=None,
                        payment_platform_key=None,
                        payment_note_code=None,
                        stage=TICKET_STAGE_AWAITING_SELECTION,
                    )
                    reset_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                    await self.audit_stage_transition(
                        trigger="user_message",
                        channel=channel,
                        message=message,
                        ticket_owner_id=owner_id,
                        ticket_record=reset_ticket_record,
                        previous_ticket_stage=ticket_stage,
                        next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                        raw_user_input=message.content,
                        normalized_user_input=normalized_input,
                        failure_reason="selected product missing during confirmation",
                    )
                    await self.send_response(message, build_ticket_retry_message())
                    return

                if not await self.send_payment_platform_prompt(message):
                    return

                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
                )
                updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                self.logger.info(
                    "ticket_selection_confirmed channel_id=%s user_id=%s script=%s timestamp=%s",
                    channel.id,
                    message.author.id,
                    current_product.key,
                    utc_timestamp(),
                )
                await self.audit_purchase_event(
                    "selection_confirmed",
                    event_category="selection",
                    status="success",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=updated_ticket_record,
                    product=current_product,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    previous_ticket_stage=ticket_stage,
                    next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
                )
                await self.audit_stage_transition(
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=updated_ticket_record,
                    previous_ticket_stage=ticket_stage,
                    next_ticket_stage=TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
                    product=current_product,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                )
                return

            selection_result = resolve_script_product_selection(message.content)
            await self.audit_purchase_event(
                "product_selection_attempted",
                event_category="selection",
                status="success",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
            )
            selected_product = selection_result.product
            if selected_product is None:
                await self.audit_purchase_event(
                    "product_selection_failed",
                    event_category="selection",
                    status="failure",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason=selection_result.status,
                    details={"candidate_keys": selection_result.candidate_keys},
                )
                await self.send_response(
                    message,
                    build_ticket_retry_message(include_confirmation_hint=True),
                )
                return

            await self.update_ticket_record(
                channel.id,
                owner_id=message.author.id,
                selected_script_key=selected_product.key,
                payment_platform_key=None,
                payment_note_code=None,
                stage=TICKET_STAGE_AWAITING_CONFIRMATION,
            )
            updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
            self.logger.info(
                "ticket_selection_updated channel_id=%s user_id=%s script=%s timestamp=%s",
                channel.id,
                message.author.id,
                selected_product.key,
                utc_timestamp(),
            )
            await self.audit_purchase_event(
                "product_selection_resolved",
                event_category="selection",
                status="success",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=updated_ticket_record,
                product=selected_product,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
                details={"candidate_keys": selection_result.candidate_keys},
            )
            await self.send_script_selection_confirmation(message, selected_product)
            return

        if ticket_stage == TICKET_STAGE_AWAITING_PAYMENT_PLATFORM:
            if current_product is None:
                await self.audit_purchase_event(
                    "ticket_state_reset",
                    event_category="state",
                    status="failure",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="selected product missing while awaiting payment platform",
                )
                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    selected_script_key=None,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_SELECTION,
                )
                reset_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                await self.audit_stage_transition(
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=reset_ticket_record,
                    previous_ticket_stage=ticket_stage,
                    next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="selected product missing while awaiting payment platform",
                )
                await self.send_response(message, build_ticket_retry_message())
                return

            await self.send_payment_platform_prompt(message)
            return

        if ticket_stage in {
            TICKET_STAGE_AWAITING_PAYMENT,
            TICKET_STAGE_PAYMENT_PENDING,
            TICKET_STAGE_COMPLETED,
        }:
            if current_product is None:
                await self.audit_purchase_event(
                    "ticket_state_reset",
                    event_category="state",
                    status="failure",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="selected product missing after confirmation",
                )
                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    selected_script_key=None,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_SELECTION,
                )
                reset_ticket_record = await self.get_ticket_record_snapshot(channel.id)
                await self.audit_stage_transition(
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=reset_ticket_record,
                    previous_ticket_stage=ticket_stage,
                    next_ticket_stage=TICKET_STAGE_AWAITING_SELECTION,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                    failure_reason="selected product missing after confirmation",
                )
                await self.send_response(message, build_ticket_retry_message())
                return

            if ticket_stage == TICKET_STAGE_COMPLETED:
                await self.audit_purchase_event(
                    "completed_ticket_notice_issued",
                    event_category="ticket",
                    status="ignored",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=current_product,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                )
                await self.send_response(
                    message,
                    (
                        f"This ticket is already completed for {current_product.label}.\n"
                        f"What happens next: it will close automatically {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes after delivery.\n"
                        "If you need another script sooner, type `close ticket` and open a new one."
                    ),
                )
                return

            if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                await self.audit_purchase_event(
                    "payment_check_pending_notice_issued",
                    event_category="payment",
                    status="ignored",
                    trigger="user_message",
                    channel=channel,
                    message=message,
                    ticket_owner_id=owner_id,
                    ticket_record=ticket_record,
                    product=current_product,
                    payment_note_code=current_payment_note_code,
                    raw_user_input=message.content,
                    normalized_user_input=normalized_input,
                )
                await self.send_response(
                    message,
                    self.build_payment_check_running_message(),
                )
                return

            await self.audit_purchase_event(
                "payment_status_reminder_issued",
                event_category="payment",
                status="success",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                product=current_product,
                platform=current_platform,
                payment_note_code=current_payment_note_code,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
            )
            effective_price = self.get_effective_ticket_price_text(
                current_product,
                ticket_record=ticket_record,
            ) or (resolve_ticket_price_text(current_product) or "0.00")
            await self.send_response(
                message,
                (
                    f"Your script is confirmed as {current_product.label} for ${effective_price}.\n"
                    f"What to do now: send payment through {current_platform.label} using the exact note code `{current_payment_note_code or 'missing'}`.\n"
                    "What happens next: after you pay, press `Check My Payment`."
                )
                if current_platform is not None
                else (
                    f"Your script is confirmed as {current_product.label} for ${effective_price}.\n"
                    "What to do now: press the payment platform button to view your payment instructions.\n"
                    "What happens next: I will show where to send payment, the exact note code, and the button to check your payment."
                ),
            )
            return

        selection_result = resolve_script_product_selection(message.content)
        await self.audit_purchase_event(
            "product_selection_attempted",
            event_category="selection",
            status="success",
            trigger="user_message",
            channel=channel,
            message=message,
            ticket_owner_id=owner_id,
            ticket_record=ticket_record,
            raw_user_input=message.content,
            normalized_user_input=normalized_input,
        )
        selected_product = selection_result.product
        if selected_product is None:
            self.logger.info(
                "ticket_selection_unmatched channel_id=%s user_id=%s timestamp=%s content=%r",
                channel.id,
                message.author.id,
                utc_timestamp(),
                message.content,
            )
            await self.audit_purchase_event(
                "product_selection_failed",
                event_category="selection",
                status="failure",
                trigger="user_message",
                channel=channel,
                message=message,
                ticket_owner_id=owner_id,
                ticket_record=ticket_record,
                raw_user_input=message.content,
                normalized_user_input=normalized_input,
                failure_reason=selection_result.status,
                details={"candidate_keys": selection_result.candidate_keys},
            )
            await self.send_response(message, build_ticket_retry_message())
            return

        await self.update_ticket_record(
            channel.id,
            owner_id=message.author.id,
            selected_script_key=selected_product.key,
            payment_platform_key=None,
            payment_note_code=None,
            stage=TICKET_STAGE_AWAITING_CONFIRMATION,
        )
        updated_ticket_record = await self.get_ticket_record_snapshot(channel.id)
        self.logger.info(
            "ticket_selection_matched channel_id=%s user_id=%s script=%s timestamp=%s",
            channel.id,
            message.author.id,
            selected_product.key,
            utc_timestamp(),
        )
        await self.audit_purchase_event(
            "product_selection_resolved",
            event_category="selection",
            status="success",
            trigger="user_message",
            channel=channel,
            message=message,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            product=selected_product,
            raw_user_input=message.content,
            normalized_user_input=normalized_input,
            details={"candidate_keys": selection_result.candidate_keys},
        )
        await self.audit_stage_transition(
            trigger="user_message",
            channel=channel,
            message=message,
            ticket_owner_id=owner_id,
            ticket_record=updated_ticket_record,
            previous_ticket_stage=ticket_stage,
            next_ticket_stage=TICKET_STAGE_AWAITING_CONFIRMATION,
            product=selected_product,
            raw_user_input=message.content,
            normalized_user_input=normalized_input,
        )
        await self.send_script_selection_confirmation(message, selected_product)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        try:
            if await self.handle_email_test_command(message):
                return
        except Exception:
            self.logger.exception(
                "email_test_command_exception channel_id=%s user_id=%s timestamp=%s",
                getattr(message.channel, "id", None),
                message.author.id,
                utc_timestamp(),
            )
            await self.send_response(
                message,
                "The email test command failed unexpectedly. Check the bot logs for details.",
            )
            return

        try:
            if await self.handle_admin_command(message):
                return
        except Exception as exc:
            channel = message.channel if isinstance(message.channel, discord.TextChannel) else None
            await self.report_purchase_flow_exception(
                event_type="admin_command_exception",
                trigger=ADMIN_COMMAND_TRIGGER,
                error=exc,
                channel=channel,
                message=message,
                raw_user_input=message.content,
                normalized_user_input=normalize_text(message.content),
                failure_reason="admin command handling raised an unexpected exception",
            )
            return

        if self.is_purchase_ticket_channel(message.channel):
            try:
                await self.handle_ticket_prompt(message)
            except Exception as exc:
                channel = (
                    message.channel
                    if isinstance(message.channel, discord.TextChannel)
                    else None
                )
                await self.report_purchase_flow_exception(
                    event_type="purchase_message_exception",
                    trigger="user_message",
                    error=exc,
                    channel=channel,
                    message=message,
                    raw_user_input=message.content,
                    normalized_user_input=normalize_text(message.content),
                    failure_reason="purchase ticket message handling raised an unexpected exception",
                )
            return
        if self.is_support_ticket_channel(message.channel):
            return

    async def close(self) -> None:
        if self.purchase_sync_retry_task is not None:
            self.purchase_sync_retry_task.cancel()
            await asyncio.gather(
                self.purchase_sync_retry_task,
                return_exceptions=True,
            )
            self.purchase_sync_retry_task = None

        if self.state_save_retry_task is not None:
            self.state_save_retry_task.cancel()
            await asyncio.gather(
                self.state_save_retry_task,
                return_exceptions=True,
            )
            self.state_save_retry_task = None

        if self.weekly_sales_report_task is not None:
            self.weekly_sales_report_task.cancel()
            await asyncio.gather(
                self.weekly_sales_report_task,
                return_exceptions=True,
            )
            self.weekly_sales_report_task = None

        if self.payment_check_tasks:
            pending_tasks = tuple(self.payment_check_tasks)
            for task in pending_tasks:
                task.cancel()
            await asyncio.gather(*pending_tasks, return_exceptions=True)
            self.payment_check_tasks.clear()
            self.pending_payment_check_channel_ids.clear()

        if self.purchase_ticket_auto_close_tasks:
            pending_auto_close_tasks = tuple(self.purchase_ticket_auto_close_tasks.values())
            for task in pending_auto_close_tasks:
                task.cancel()
            await asyncio.gather(*pending_auto_close_tasks, return_exceptions=True)
            self.purchase_ticket_auto_close_tasks.clear()

        self.audit_logger.close()
        await super().close()
