from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _parse_csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    values = tuple(
        item.strip().lower()
        for item in raw_value.split(",")
        if item.strip()
    )
    return values or default


def _parse_csv_preserve_case_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    values = tuple(item.strip() for item in raw_value.split(",") if item.strip())
    return values or default


def _parse_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value.strip())
    except ValueError:
        return default


def _parse_decimal_env(name: str, default: Decimal) -> Decimal:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return Decimal(raw_value.strip())
    except (InvalidOperation, ValueError):
        return default


def _resolve_path(value: str | Path, *, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _parse_path_env(name: str, default: str | Path, *, base_dir: Path = REPO_ROOT) -> Path:
    raw_value = os.getenv(name)
    if raw_value is None:
        return _resolve_path(default, base_dir=base_dir)
    value = raw_value.strip()
    if not value:
        return _resolve_path(default, base_dir=base_dir)
    return _resolve_path(value, base_dir=base_dir)


RUNTIME_DIR = _parse_path_env("DC_BOT_RUNTIME_DIR", "runtime")
DATA_DIR = _parse_path_env("DC_BOT_DATA_DIR", RUNTIME_DIR / "data")
LOG_DIR = _parse_path_env("DC_BOT_LOG_DIR", RUNTIME_DIR / "logs")
STATE_FILE = _parse_path_env("DC_BOT_STATE_FILE", DATA_DIR / "dc_bot_state.json")
STATE_BACKUP_FILE = _parse_path_env(
    "DC_BOT_STATE_BACKUP_FILE",
    DATA_DIR / "dc_bot_state.backup.json",
)
LOG_FILE = _parse_path_env("DC_BOT_LOG_FILE", LOG_DIR / "dc_bot.log")
PAYMENT_PARSER_LOG_FILE = _parse_path_env(
    "DC_BOT_PAYMENT_PARSER_LOG_FILE",
    LOG_DIR / "payment_parser.log",
)
PURCHASE_LOG_FILE = _parse_path_env(
    "DC_BOT_PURCHASE_LOG_FILE",
    DATA_DIR / "purchase_log.jsonl",
)
PURCHASE_SYNC_RECOVERY_FILE = _parse_path_env(
    "DC_BOT_PURCHASE_SYNC_RECOVERY_FILE",
    DATA_DIR / "purchase_sync_recovery.jsonl",
)
GOOGLE_SHEETS_CREDENTIALS_JSON_ENV = "GOOGLE_SHEETS_CREDENTIALS_JSON"
GOOGLE_SHEETS_CREDENTIALS_FILE = _parse_path_env(
    "GOOGLE_SHEETS_CREDENTIALS_FILE",
    "credentials/google_service_account.json",
)
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
GOOGLE_SHEETS_TAB_NAME = os.getenv("GOOGLE_SHEETS_TAB_NAME", "Log").strip() or "Log"
GOOGLE_SHEETS_AUDIT_TAB_NAME = (
    os.getenv("GOOGLE_SHEETS_AUDIT_TAB_NAME", "Purchase Audit").strip()
    or "Purchase Audit"
)
GOOGLE_SHEETS_ERROR_TAB_NAME = (
    os.getenv("GOOGLE_SHEETS_ERROR_TAB_NAME", "Bot Errors").strip() or "Bot Errors"
)
GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
PURCHASE_SYNC_RETRY_INTERVAL_SECONDS = 300
STATE_SAVE_RETRY_INTERVAL_SECONDS = _parse_int_env(
    "DC_BOT_STATE_SAVE_RETRY_INTERVAL_SECONDS",
    60,
)

GMAIL_API_CLIENT_ID_ENV = "GMAIL_API_CLIENT_ID"
GMAIL_API_CLIENT_SECRET_ENV = "GMAIL_API_CLIENT_SECRET"
GMAIL_API_REFRESH_TOKEN_ENV = "GMAIL_API_REFRESH_TOKEN"
GMAIL_API_TOKEN_URI = (
    os.getenv("GMAIL_API_TOKEN_URI", "https://oauth2.googleapis.com/token").strip()
    or "https://oauth2.googleapis.com/token"
)
GMAIL_API_USER_ID = "me"
GMAIL_API_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/gmail.readonly",
)
PAYMENT_PARSER_GMAIL_ADDRESS = (
    os.getenv("PAYMENT_PARSER_GMAIL_ADDRESS", "payments@example.com").strip()
    or "payments@example.com"
)
PAYMENT_PARSER_EXPECTED_AMOUNT = _parse_decimal_env(
    "PAYMENT_PARSER_EXPECTED_AMOUNT",
    Decimal("23.00"),
)
PAYMENT_PARSER_TIMEOUT_SECONDS = _parse_int_env(
    "PAYMENT_PARSER_TIMEOUT_SECONDS",
    60,
)
ALLOWED_FROM_DOMAINS = _parse_csv_env(
    "PAYMENT_PARSER_ALLOWED_FROM_DOMAINS",
    ("cash.app", "square.com", "squareup.com"),
)
ALLOWED_FROM_SUBDOMAINS = _parse_csv_env(
    "PAYMENT_PARSER_ALLOWED_FROM_SUBDOMAINS",
    (),
)
ALLOWED_FROM_ADDRESSES = _parse_csv_env(
    "PAYMENT_PARSER_ALLOWED_FROM_ADDRESSES",
    (),
)
ALLOWED_CURRENCY = (
    os.getenv("PAYMENT_PARSER_ALLOWED_CURRENCY", "USD").strip().upper() or "USD"
)
MAX_MESSAGES_TO_SCAN = _parse_int_env("PAYMENT_PARSER_MAX_MESSAGES_TO_SCAN", 50)
MAX_MESSAGE_AGE_HOURS = _parse_int_env("PAYMENT_PARSER_MAX_MESSAGE_AGE_HOURS", 24)
NEGATIVE_TIME_BUFFER_MINUTES = _parse_int_env(
    "PAYMENT_PARSER_NEGATIVE_TIME_BUFFER_MINUTES",
    2,
)
POSITIVE_TIME_WINDOW_MINUTES = _parse_int_env(
    "PAYMENT_PARSER_POSITIVE_TIME_WINDOW_MINUTES",
    20,
)
REQUIRE_DMARC_WHEN_AVAILABLE = _parse_bool_env(
    "PAYMENT_PARSER_REQUIRE_DMARC_WHEN_AVAILABLE",
    True,
)
REQUIRE_STRICT_ALIGNMENT = _parse_bool_env(
    "PAYMENT_PARSER_REQUIRE_STRICT_ALIGNMENT",
    True,
)
REQUIRE_STRICT_FROM_ADDRESS_ALLOWLIST = _parse_bool_env(
    "PAYMENT_PARSER_REQUIRE_STRICT_FROM_ADDRESS_ALLOWLIST",
    False,
)
REJECT_FORWARDED = _parse_bool_env("PAYMENT_PARSER_REJECT_FORWARDED", True)
REJECT_RESENT = _parse_bool_env("PAYMENT_PARSER_REJECT_RESENT", True)
REJECT_PASTED_STRONG_ONLY = _parse_bool_env(
    "PAYMENT_PARSER_REJECT_PASTED_STRONG_ONLY",
    True,
)
CONSUMED_MESSAGE_ID_RETENTION_DAYS = _parse_int_env(
    "PAYMENT_PARSER_CONSUMED_MESSAGE_ID_RETENTION_DAYS",
    30,
)

ADMIN_EMAIL_RECIPIENTS = _parse_csv_preserve_case_env(
    "ADMIN_EMAIL_RECIPIENTS",
    (),
)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com"
SMTP_PORT = _parse_int_env("SMTP_PORT", 587)
SMTP_SENDER_ADDRESS = (
    os.getenv("SMTP_SENDER_ADDRESS", "").strip()
)
SMTP_PASSWORD_ENV = "SMTP_PASSWORD"
SMTP_TIMEOUT_SECONDS = _parse_int_env("SMTP_TIMEOUT_SECONDS", 30)
WEEKLY_SALES_REPORT_WEEKDAY = (
    os.getenv("WEEKLY_SALES_REPORT_WEEKDAY", "monday").strip().lower() or "monday"
)
WEEKLY_SALES_REPORT_HOUR = _parse_int_env("WEEKLY_SALES_REPORT_HOUR", 9)
WEEKLY_SALES_REPORT_MINUTE = _parse_int_env("WEEKLY_SALES_REPORT_MINUTE", 0)
WEEKLY_SALES_REPORT_TIMEZONE = (
    os.getenv("WEEKLY_SALES_REPORT_TIMEZONE", "America/New_York").strip()
    or "America/New_York"
)

DISCORD_MESSAGE_LIMIT = 2000
TICKET_PANEL_CHANNEL_ID = _parse_int_env("TICKET_PANEL_CHANNEL_ID", 0)
TICKET_CATEGORY_ID = _parse_int_env("TICKET_CATEGORY_ID", 0)
TICKET_BUTTON_CUSTOM_ID = "dc_bot:open_ticket"
SUPPORT_TICKET_PANEL_CHANNEL_ID = _parse_int_env("SUPPORT_TICKET_PANEL_CHANNEL_ID", 0)
SUPPORT_TICKET_CATEGORY_ID = _parse_int_env("SUPPORT_TICKET_CATEGORY_ID", 0)
SUPPORT_MODERATOR_ROLE_ID = _parse_int_env("SUPPORT_MODERATOR_ROLE_ID", 0)
SUPPORT_TICKET_BUTTON_CUSTOM_ID = "dc_bot:open_support_ticket"
PAYMENT_PLATFORM_BUTTON_CUSTOM_ID_PREFIX = "dc_bot:payment_platform"
PAYMENT_BUTTON_CUSTOM_ID = "dc_bot:confirm_payment"
PAYMENT_CHECK_DELAY_SECONDS = 40
CASH_APP_CASHTAG = os.getenv("CASH_APP_CASHTAG", "$CHANGE_ME").strip() or "$CHANGE_ME"
CONFIRM_SELECTION_RESPONSE = "yes"
SUPPORT_TICKET_PANEL_MESSAGE = (
    "Need help with payment or delivery?\n"
    "Press the button below to open a private support ticket and message a moderator."
)
SUPPORT_TICKET_CHANNEL_MESSAGE = (
    f"<@&{SUPPORT_MODERATOR_ROLE_ID}>\n"
    "A new support ticket has been opened.\n\n"
    "Please send the following details in one message right away so we can review this faster:\n\n"
    "- The platform you used\n"
    "- Your account name or the payment address used to send the payment\n"
    "- The time the payment was sent\n"
    "- What went wrong\n\n"
    "A moderator will review your message and assist you shortly."
)
