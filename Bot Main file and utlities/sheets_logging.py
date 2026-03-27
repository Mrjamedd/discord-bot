from __future__ import annotations

import json
import os
import queue
import sys
import threading
from pathlib import Path
from typing import Any, Sequence

from google.oauth2 import service_account
from googleapiclient.discovery import build

_WORKER_STOP = object()


def truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    if max_chars <= 32:
        return value[:max_chars]
    return value[: max_chars - 17] + "\n...[truncated]"


class QueuedGoogleSheetsTabWriter:
    def __init__(
        self,
        *,
        credentials_path: Path,
        credentials_json_env_name: str,
        sheet_tab_name: str,
        sheets_scope: str,
        spreadsheet_id: str,
        header: Sequence[str],
        failure_notice_prefix: str,
    ) -> None:
        self.credentials_path = credentials_path
        self.credentials_json_env_name = credentials_json_env_name
        self.sheet_tab_name = sheet_tab_name
        self.sheets_scope = sheets_scope
        self.spreadsheet_id = spreadsheet_id
        self.header = list(header)
        self.failure_notice_prefix = failure_notice_prefix
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

    def sheet_logging_state(self) -> tuple[bool, str | None]:
        has_spreadsheet_id = bool(self.spreadsheet_id)
        has_inline_credentials = bool(self._inline_credentials_json())
        has_credentials_file = self.credentials_path.is_file()

        if has_spreadsheet_id and (has_inline_credentials or has_credentials_file):
            return True, None
        if not has_spreadsheet_id and not has_inline_credentials and not has_credentials_file:
            return False, None
        if not has_spreadsheet_id:
            return False, "GOOGLE_SHEETS_SPREADSHEET_ID is not set"
        return (
            False,
            (
                f"{self.credentials_json_env_name} is empty and credentials file is missing at "
                f"{self.credentials_path}"
            ),
        )

    def _get_credentials(self) -> service_account.Credentials:
        if self._credentials is None:
            inline_credentials_json = self._inline_credentials_json()
            if inline_credentials_json:
                credentials_info = json.loads(inline_credentials_json)
                self._credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=[self.sheets_scope],
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
        if first_row == self.header:
            self._header_ready = True
            return
        if first_row and first_row[0] != self.header[0]:
            self._header_ready = True
            return

        last_column = self._column_number_to_a1(len(self.header))
        (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_tab_name}!A1:{last_column}1",
                valueInputOption="RAW",
                body={"values": [self.header]},
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
                name=f"dc-bot-sheets-{self.sheet_tab_name}",
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
                    f"{self.failure_notice_prefix} failed: {exc}"
                )

    def enqueue_row(self, row: Sequence[object]) -> bool:
        if self._closed:
            return False

        sheet_logging_enabled, _ = self.sheet_logging_state()
        if not sheet_logging_enabled:
            return False

        try:
            self._ensure_worker_started()
            self._queue.put_nowait(
                ["" if value is None else str(value) for value in row]
            )
            return True
        except Exception as exc:
            self._write_failure_notice(
                f"{self.failure_notice_prefix} enqueue failed: {exc}"
            )
            return False

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        if self._worker_thread is not None:
            self._queue.put(_WORKER_STOP)
            self._worker_thread.join(timeout=5)
            self._worker_thread = None
