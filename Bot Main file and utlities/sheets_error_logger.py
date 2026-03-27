from __future__ import annotations

import json
import logging
import re
import traceback
from datetime import datetime, timezone

from config import (
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
    GOOGLE_SHEETS_ERROR_TAB_NAME,
    GOOGLE_SHEETS_SCOPE,
    GOOGLE_SHEETS_SPREADSHEET_ID,
)
from sheets_logging import QueuedGoogleSheetsTabWriter, truncate_text

ERROR_LOG_COLUMNS: tuple[str, ...] = (
    "Logged At (UTC)",
    "Level",
    "Logger",
    "Module",
    "Function",
    "Line No",
    "Event",
    "Raw Message",
    "Exception Type",
    "Exception Message",
    "User ID",
    "Channel ID",
    "Guild ID",
    "Purchase Event ID",
    "Item Key",
    "Gmail Message ID",
    "Context JSON",
    "Traceback",
)
_CONTEXT_FIELD_PATTERN = re.compile(r"(?<!\S)([A-Za-z0-9_]+)=")
_MAX_RAW_MESSAGE_CHARS = 8_000
_MAX_EXCEPTION_MESSAGE_CHARS = 4_000
_MAX_CONTEXT_CHARS = 16_000
_MAX_TRACEBACK_CHARS = 40_000


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_event_and_context(message: str) -> tuple[str, dict[str, str]]:
    stripped_message = message.strip()
    if not stripped_message:
        return "unlabeled", {}

    first_token, _, remainder = stripped_message.partition(" ")
    if "=" in first_token:
        event_name = "unstructured"
        searchable_message = stripped_message
    else:
        event_name = first_token
        searchable_message = remainder.strip()

    matches = list(_CONTEXT_FIELD_PATTERN.finditer(searchable_message))
    if not matches:
        return event_name, {}

    context: dict[str, str] = {}
    for index, match in enumerate(matches):
        value_start = match.end()
        value_end = (
            matches[index + 1].start()
            if index + 1 < len(matches)
            else len(searchable_message)
        )
        context[match.group(1)] = searchable_message[value_start:value_end].strip()
    return event_name, context


def _context_value(context: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = context.get(key, "")
        if value:
            return value
    return ""


def _exception_details(record: logging.LogRecord) -> tuple[str, str, str]:
    if record.exc_info is not None:
        exc_type = record.exc_info[0].__name__ if record.exc_info[0] is not None else ""
        exc_message = (
            str(record.exc_info[1]) if record.exc_info[1] is not None else ""
        )
        traceback_text = "".join(traceback.format_exception(*record.exc_info))
        return exc_type, exc_message, traceback_text
    if record.stack_info:
        return "", "", record.stack_info
    return "", "", ""


def _build_error_sheet_row(record: logging.LogRecord) -> list[str]:
    raw_message = record.getMessage()
    event_name, context = _extract_event_and_context(raw_message)
    exception_type, exception_message, traceback_text = _exception_details(record)
    context_json = json.dumps(context, ensure_ascii=False, sort_keys=True)
    return [
        _utc_timestamp(),
        record.levelname,
        record.name,
        record.module,
        record.funcName,
        str(record.lineno),
        event_name,
        truncate_text(raw_message, _MAX_RAW_MESSAGE_CHARS),
        exception_type,
        truncate_text(exception_message, _MAX_EXCEPTION_MESSAGE_CHARS),
        _context_value(context, "user_id"),
        _context_value(context, "channel_id"),
        _context_value(context, "guild_id"),
        _context_value(context, "purchase_event_id"),
        _context_value(context, "item_key", "script"),
        _context_value(context, "gmail_message_id"),
        truncate_text(context_json, _MAX_CONTEXT_CHARS),
        truncate_text(traceback_text, _MAX_TRACEBACK_CHARS),
    ]


class GoogleSheetsErrorHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self._writer = QueuedGoogleSheetsTabWriter(
            credentials_path=GOOGLE_SHEETS_CREDENTIALS_FILE,
            credentials_json_env_name=GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
            sheet_tab_name=GOOGLE_SHEETS_ERROR_TAB_NAME,
            sheets_scope=GOOGLE_SHEETS_SCOPE,
            spreadsheet_id=GOOGLE_SHEETS_SPREADSHEET_ID,
            header=ERROR_LOG_COLUMNS,
            failure_notice_prefix="Google Sheets error logging",
        )

    def emit(self, record: logging.LogRecord) -> None:
        row = _build_error_sheet_row(record)
        self._writer.enqueue_row(row)

    def close(self) -> None:
        self._writer.close()
        super().close()


__all__ = [
    "ERROR_LOG_COLUMNS",
    "GoogleSheetsErrorHandler",
    "_build_error_sheet_row",
    "_extract_event_and_context",
]
