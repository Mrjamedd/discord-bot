from __future__ import annotations

import os

from Email_Parser import private_email_parser_config_error
from admin_email import AdminEmailNotifier
from assets import validate_script_asset_directory
from bot import DiscordPurchaseBot
from config import (
    ADMIN_EMAIL_RECIPIENTS,
    CASH_APP_CASHTAG,
    GMAIL_API_CLIENT_ID_ENV,
    GMAIL_API_CLIENT_SECRET_ENV,
    GMAIL_API_REFRESH_TOKEN_ENV,
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
    GOOGLE_SHEETS_SPREADSHEET_ID,
    PAYMENT_PARSER_GMAIL_ADDRESS,
    SMTP_PASSWORD_ENV,
    SUPPORT_MODERATOR_ROLE_ID,
    SUPPORT_TICKET_CATEGORY_ID,
    SUPPORT_TICKET_PANEL_CHANNEL_ID,
    TICKET_CATEGORY_ID,
    TICKET_PANEL_CHANNEL_ID,
)
from purchase_audit_logger import PurchaseFlowAuditLogger
from purchase_logger import PurchaseLogger
from ticketing import SCRIPT_PRODUCTS
from utils import setup_logger


def _runtime_configuration_errors() -> list[str]:
    errors: list[str] = []

    discord_token = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    if not discord_token:
        errors.append("DISCORD_BOT_TOKEN is not set.")

    required_channel_ids = {
        "TICKET_PANEL_CHANNEL_ID": TICKET_PANEL_CHANNEL_ID,
        "TICKET_CATEGORY_ID": TICKET_CATEGORY_ID,
        "SUPPORT_TICKET_PANEL_CHANNEL_ID": SUPPORT_TICKET_PANEL_CHANNEL_ID,
        "SUPPORT_TICKET_CATEGORY_ID": SUPPORT_TICKET_CATEGORY_ID,
        "SUPPORT_MODERATOR_ROLE_ID": SUPPORT_MODERATOR_ROLE_ID,
    }
    for env_name, value in required_channel_ids.items():
        if value <= 0:
            errors.append(f"{env_name} must be set to a Discord ID.")

    if not PAYMENT_PARSER_GMAIL_ADDRESS or PAYMENT_PARSER_GMAIL_ADDRESS.endswith("@example.com"):
        errors.append("PAYMENT_PARSER_GMAIL_ADDRESS must be set to the Gmail inbox used for payment receipts.")

    for env_name in (
        GMAIL_API_CLIENT_ID_ENV,
        GMAIL_API_CLIENT_SECRET_ENV,
        GMAIL_API_REFRESH_TOKEN_ENV,
    ):
        if not (os.getenv(env_name) or "").strip():
            errors.append(f"{env_name} is not set.")

    parser_config_error = private_email_parser_config_error()
    if parser_config_error is not None:
        errors.append(parser_config_error)

    if not CASH_APP_CASHTAG or CASH_APP_CASHTAG == "$CHANGE_ME":
        errors.append("CASH_APP_CASHTAG must be set to the payment destination you advertise to customers.")

    errors.extend(validate_script_asset_directory(SCRIPT_PRODUCTS))
    return errors


def _runtime_configuration_warnings() -> list[str]:
    warnings: list[str] = []
    has_spreadsheet_id = bool(GOOGLE_SHEETS_SPREADSHEET_ID)
    has_inline_credentials = bool((os.getenv(GOOGLE_SHEETS_CREDENTIALS_JSON_ENV) or "").strip())
    has_credentials_file = GOOGLE_SHEETS_CREDENTIALS_FILE.is_file()
    if has_spreadsheet_id and not (has_inline_credentials or has_credentials_file):
        warnings.append(
            "Google Sheets sync is disabled because no credentials were provided. "
            "Completed purchases will still be logged locally, but purchase audit logging and structured error reports will not be written to Google Sheets."
        )
    elif (has_inline_credentials or has_credentials_file) and not has_spreadsheet_id:
        warnings.append(
            "Google Sheets credentials are present but GOOGLE_SHEETS_SPREADSHEET_ID is not set. "
            "Completed purchases will still be logged locally, but purchase audit logging and structured error reports will not be written to Google Sheets."
        )
    if not (os.getenv(SMTP_PASSWORD_ENV) or "").strip():
        warnings.append(
            "Admin email notifications are disabled because SMTP_PASSWORD is not set."
        )
    if not ADMIN_EMAIL_RECIPIENTS:
        warnings.append(
            "Admin email notifications are disabled because no admin recipients are configured."
        )
    return warnings


def main() -> int:
    configuration_errors = _runtime_configuration_errors()
    if configuration_errors:
        print("Configuration errors:")
        for error in configuration_errors:
            print(f"- {error}")
        return 1
    discord_token = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()

    logger = setup_logger()
    for warning in _runtime_configuration_warnings():
        logger.warning("startup_configuration_warning %s", warning)
    purchase_logger = PurchaseLogger(logger)
    audit_logger = PurchaseFlowAuditLogger(logger)
    admin_email_notifier = AdminEmailNotifier(logger)
    bot = DiscordPurchaseBot(
        logger=logger,
        purchase_logger=purchase_logger,
        audit_logger=audit_logger,
        admin_email_notifier=admin_email_notifier,
    )
    bot.run(discord_token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
