from __future__ import annotations

import json
import logging
import os
import queue
import re
import sys
import threading
import traceback
from datetime import datetime, timezone
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import (
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
    GOOGLE_SHEETS_ERROR_TAB_NAME,
    GOOGLE_SHEETS_SCOPE,
    GOOGLE_SHEETS_SPREADSHEET_ID,
)

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
_WORKER_STOP = object()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    if max_chars <= 32:
        return value[:max_chars]
    return value[: max_chars - 17] + "\n...[truncated]"


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
        _truncate(raw_message, _MAX_RAW_MESSAGE_CHARS),
        exception_type,
        _truncate(exception_message, _MAX_EXCEPTION_MESSAGE_CHARS),
        _context_value(context, "user_id"),
        _context_value(context, "channel_id"),
        _context_value(context, "guild_id"),
        _context_value(context, "purchase_event_id"),
        _context_value(context, "item_key", "script"),
        _context_value(context, "gmail_message_id"),
        _truncate(context_json, _MAX_CONTEXT_CHARS),
        _truncate(traceback_text, _MAX_TRACEBACK_CHARS),
    ]


class GoogleSheetsErrorHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.credentials_path = GOOGLE_SHEETS_CREDENTIALS_FILE
        self.credentials_json_env_name = GOOGLE_SHEETS_CREDENTIALS_JSON_ENV
        self.sheet_tab_name = GOOGLE_SHEETS_ERROR_TAB_NAME
        self.sheets_scope = GOOGLE_SHEETS_SCOPE
        self.spreadsheet_id = GOOGLE_SHEETS_SPREADSHEET_ID
        self._credentials: service_account.Credentials | None = None
        self._sheets_service: Any | None = None
        self._header_ready = False
        self._worker_lock = threading.Lock()
        self._queue: queue.Queue[list[str] | object] = queue.Queue()
        self._worker_thread: threading.Thread | None = None
        self._closed = False
        self._failure_notice_emitted = False

    def _inline_credentials_json(self) -> str:
        return os.getenv(self.credentials_json_env_name, "").strip()

    def _sheet_logging_enabled(self) -> bool:
        if not self.spreadsheet_id or not self.sheet_tab_name:
            return False
        if self._inline_credentials_json():
            return True
        return self.credentials_path.is_file()

    def _get_credentials(self) -> service_account.Credentials:
        if self._credentials is None:
            inline_credentials_json = self._inline_credentials_json()
            if inline_credentials_json:
                credentials_info = json.loads(inline_credentials_json)
                self._credentials = (
                    service_account.Credentials.from_service_account_info(
                        credentials_info,
                        scopes=[self.sheets_scope],
                    )
                )
            else:
                self._credentials = service_account.Credentials.from_service_account_file(
                    str(self.credentials_path),
                    scopes=[self.sheets_scope],
                )
        return self._credentials

    def _get_sheets_service(self) -> Any:
        if self._sheets_service is None:
            self._sheets_service = build(
                "sheets",
                "v4",
                credentials=self._get_credentials(),
                cache_discovery=False,
            )
        return self._sheets_service

    def _column_number_to_a1(self, column_number: int) -> str:
        letters = ""
        remaining = column_number
        while remaining > 0:
            remaining, remainder = divmod(remaining - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def _get_sheet_titles(self, sheets_service: Any) -> set[str]:
        response = (
            sheets_service.spreadsheets()
            .get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets(properties(title))",
            )
            .execute()
        )
        raw_sheets = response.get("sheets", [])
        if not isinstance(raw_sheets, list):
            return set()

        titles: set[str] = set()
        for sheet in raw_sheets:
            if not isinstance(sheet, dict):
                continue
            properties = sheet.get("properties")
            if not isinstance(properties, dict):
                continue
            title = properties.get("title")
            if isinstance(title, str) and title:
                titles.add(title)
        return titles

    def _ensure_sheet_exists(self, sheets_service: Any) -> None:
        sheet_titles = self._get_sheet_titles(sheets_service)
        if self.sheet_tab_name in sheet_titles:
            return

        try:
            (
                sheets_service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={
                        "requests": [
                            {"addSheet": {"properties": {"title": self.sheet_tab_name}}}
                        ]
                    },
                )
                .execute()
            )
            return
        except Exception:
            # A concurrent creator may have already added the sheet.
            sheet_titles = self._get_sheet_titles(sheets_service)
            if self.sheet_tab_name in sheet_titles:
                return
            raise

    def _ensure_sheet_header(self, sheets_service: Any) -> None:
        if self._header_ready:
            return

        self._ensure_sheet_exists(sheets_service)
        header_response = (
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_tab_name}!1:1",
            )
            .execute()
        )
        values = header_response.get("values", [])
        first_row = [str(cell) for cell in values[0]] if isinstance(values, list) and values else []
        expected_header = list(ERROR_LOG_COLUMNS)
        if first_row == expected_header:
            self._header_ready = True
            return
        if first_row and first_row[0] != expected_header[0]:
            self._header_ready = True
            return

        last_column = self._column_number_to_a1(len(ERROR_LOG_COLUMNS))
        (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_tab_name}!A1:{last_column}1",
                valueInputOption="RAW",
                body={"values": [expected_header]},
            )
            .execute()
        )
        self._header_ready = True

    def _append_row(self, row: list[str]) -> None:
        sheets_service = self._get_sheets_service()
        self._ensure_sheet_header(sheets_service)
        (
            sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=self.sheet_tab_name,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            )
            .execute()
        )

    def _ensure_worker_started(self) -> None:
        if self._worker_thread is not None:
            return
        with self._worker_lock:
            if self._worker_thread is not None:
                return
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="dc-bot-sheets-error-logger",
                daemon=True,
            )
            self._worker_thread.start()

    def _write_failure_notice(self, message: str) -> None:
        if self._failure_notice_emitted:
            return
        self._failure_notice_emitted = True
        print(message, file=sys.stderr)

    def _worker_loop(self) -> None:
        while True:
            row = self._queue.get()
            if row is _WORKER_STOP:
                return
            try:
                self._append_row(row)
            except Exception as exc:
                self._write_failure_notice(
                    f"Google Sheets error logging failed: {exc}"
                )

    def emit(self, record: logging.LogRecord) -> None:
        if self._closed or not self._sheet_logging_enabled():
            return
        try:
            row = _build_error_sheet_row(record)
            self._ensure_worker_started()
            self._queue.put_nowait(row)
        except Exception as exc:
            self._write_failure_notice(
                f"Google Sheets error logging enqueue failed: {exc}"
            )

    def close(self) -> None:
        if self._closed:
            super().close()
            return

        self._closed = True
        if self._worker_thread is not None:
            self._queue.put(_WORKER_STOP)
            self._worker_thread.join(timeout=5)
            self._worker_thread = None
        super().close()


__all__ = [
    "ERROR_LOG_COLUMNS",
    "GoogleSheetsErrorHandler",
    "_build_error_sheet_row",
    "_extract_event_and_context",
]
