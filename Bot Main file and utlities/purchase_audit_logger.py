from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Mapping

from config import (
    GOOGLE_SHEETS_AUDIT_TAB_NAME,
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
    GOOGLE_SHEETS_SCOPE,
    GOOGLE_SHEETS_SPREADSHEET_ID,
)
from sheets_logging import QueuedGoogleSheetsTabWriter, truncate_text

PURCHASE_AUDIT_COLUMNS: tuple[str, ...] = (
    "Logged At (UTC)",
    "Logged At (Human UTC)",
    "Event Type",
    "Event Category",
    "Status",
    "Trigger",
    "Ticket Stage",
    "Previous Ticket Stage",
    "Next Ticket Stage",
    "Discord User ID",
    "Discord Username",
    "Discord Display Name",
    "Ticket Owner ID",
    "Ticket Owner Username",
    "Channel ID",
    "Channel Name",
    "Guild ID",
    "Guild Name",
    "Message ID",
    "Interaction ID",
    "Button Custom ID",
    "Raw User Input",
    "Normalized User Input",
    "Selected Product Key",
    "Selected Product Label",
    "Selected Product Filename",
    "Selected Price",
    "Payment Platform Key",
    "Payment Platform Label",
    "Payment Note Code",
    "Delivery Filename",
    "Gmail Message ID",
    "Purchase Event ID",
    "Failure Reason",
    "Error Type",
    "Error Message",
    "Details JSON",
)
_MAX_INPUT_CHARS = 4_000
_MAX_ERROR_CHARS = 8_000
_MAX_DETAILS_CHARS = 16_000


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _human_utc_timestamp(timestamp: str) -> str:
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return timestamp

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def _string_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _details_json(value: object) -> str:
    if value in (None, "", {}):
        return ""
    if isinstance(value, str):
        return truncate_text(value, _MAX_DETAILS_CHARS)
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return truncate_text(serialized, _MAX_DETAILS_CHARS)


def _build_purchase_audit_row(event: Mapping[str, object]) -> list[str]:
    logged_at_utc = _string_value(event.get("logged_at_utc")) or _utc_timestamp()
    return [
        logged_at_utc,
        _human_utc_timestamp(logged_at_utc),
        _string_value(event.get("event_type")),
        _string_value(event.get("event_category")),
        _string_value(event.get("status")),
        _string_value(event.get("trigger")),
        _string_value(event.get("ticket_stage")),
        _string_value(event.get("previous_ticket_stage")),
        _string_value(event.get("next_ticket_stage")),
        _string_value(event.get("discord_user_id")),
        _string_value(event.get("discord_username")),
        _string_value(event.get("discord_display_name")),
        _string_value(event.get("ticket_owner_id")),
        _string_value(event.get("ticket_owner_username")),
        _string_value(event.get("channel_id")),
        _string_value(event.get("channel_name")),
        _string_value(event.get("guild_id")),
        _string_value(event.get("guild_name")),
        _string_value(event.get("message_id")),
        _string_value(event.get("interaction_id")),
        _string_value(event.get("button_custom_id")),
        truncate_text(_string_value(event.get("raw_user_input")), _MAX_INPUT_CHARS),
        truncate_text(
            _string_value(event.get("normalized_user_input")),
            _MAX_INPUT_CHARS,
        ),
        _string_value(event.get("selected_product_key")),
        _string_value(event.get("selected_product_label")),
        _string_value(event.get("selected_product_filename")),
        _string_value(event.get("selected_price")),
        _string_value(event.get("payment_platform_key")),
        _string_value(event.get("payment_platform_label")),
        _string_value(event.get("payment_note_code")),
        _string_value(event.get("delivery_filename")),
        _string_value(event.get("gmail_message_id")),
        _string_value(event.get("purchase_event_id")),
        _string_value(event.get("failure_reason")),
        _string_value(event.get("error_type")),
        truncate_text(_string_value(event.get("error_message")), _MAX_ERROR_CHARS),
        _details_json(event.get("details")),
    ]


class PurchaseFlowAuditLogger:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.sheet_tab_name = GOOGLE_SHEETS_AUDIT_TAB_NAME
        self.spreadsheet_id = GOOGLE_SHEETS_SPREADSHEET_ID
        self.credentials_path = GOOGLE_SHEETS_CREDENTIALS_FILE
        self._sheet_logging_warning_emitted = False
        self._writer = QueuedGoogleSheetsTabWriter(
            credentials_path=GOOGLE_SHEETS_CREDENTIALS_FILE,
            credentials_json_env_name=GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
            sheet_tab_name=GOOGLE_SHEETS_AUDIT_TAB_NAME,
            sheets_scope=GOOGLE_SHEETS_SCOPE,
            spreadsheet_id=GOOGLE_SHEETS_SPREADSHEET_ID,
            header=PURCHASE_AUDIT_COLUMNS,
            failure_notice_prefix="Google Sheets purchase audit logging",
        )

    def should_log(self) -> bool:
        sheet_logging_enabled, disabled_reason = self._writer.sheet_logging_state()
        if sheet_logging_enabled:
            return True
        if disabled_reason is not None and not self._sheet_logging_warning_emitted:
            self._sheet_logging_warning_emitted = True
            self.logger.warning(
                "purchase_audit_sheet_sync_disabled reason=%s spreadsheet_id=%s sheet=%s credentials_path=%s",
                disabled_reason,
                self.spreadsheet_id,
                self.sheet_tab_name,
                self.credentials_path,
            )
        return False

    def log_event(self, event: Mapping[str, object]) -> None:
        if not self.should_log():
            return
        try:
            row = _build_purchase_audit_row(event)
        except Exception:
            self.logger.exception(
                "purchase_audit_row_build_failed event_type=%s",
                event.get("event_type"),
            )
            return
        self._writer.enqueue_row(row)

    def close(self) -> None:
        self._writer.close()


__all__ = [
    "PURCHASE_AUDIT_COLUMNS",
    "PurchaseFlowAuditLogger",
    "_build_purchase_audit_row",
]
