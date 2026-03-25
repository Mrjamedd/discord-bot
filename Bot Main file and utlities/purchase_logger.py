from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from threading import Lock
from typing import Any, cast

from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import (
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_CREDENTIALS_JSON_ENV,
    GOOGLE_SHEETS_SCOPE,
    GOOGLE_SHEETS_SPREADSHEET_ID,
    GOOGLE_SHEETS_TAB_NAME,
    PURCHASE_LOG_FILE,
    PURCHASE_SYNC_RECOVERY_FILE,
)
from models import PURCHASE_LOG_COLUMNS, PurchaseRecord
from utils import ensure_parent_directory, utc_timestamp

class PurchaseLogger:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.purchase_log_file = PURCHASE_LOG_FILE
        self.recovery_file = PURCHASE_SYNC_RECOVERY_FILE
        self.credentials_path = GOOGLE_SHEETS_CREDENTIALS_FILE
        self.spreadsheet_id = GOOGLE_SHEETS_SPREADSHEET_ID
        self.sheet_tab_name = GOOGLE_SHEETS_TAB_NAME
        self.sheets_scope = GOOGLE_SHEETS_SCOPE
        self.credentials_json_env_name = GOOGLE_SHEETS_CREDENTIALS_JSON_ENV
        self._credentials: service_account.Credentials | None = None
        self._sheets_service: Any | None = None
        self._lock = Lock()
        self._sheet_logging_warning_emitted = False

    def _inline_credentials_json(self) -> str:
        return os.getenv(self.credentials_json_env_name, "").strip()

    def _sheet_logging_state(self) -> tuple[bool, str | None]:
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

    def _warn_sheet_logging_disabled_once(self, reason: str) -> None:
        if self._sheet_logging_warning_emitted:
            return
        self._sheet_logging_warning_emitted = True
        self.logger.warning(
            "purchase_log_sheet_sync_disabled reason=%s spreadsheet_id=%s sheet=%s credentials_path=%s",
            reason,
            self.spreadsheet_id,
            self.sheet_tab_name,
            self.credentials_path,
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

    def _build_sheet_row(self, record: PurchaseRecord) -> list[str | int]:
        return [record[column] for column in PURCHASE_LOG_COLUMNS]

    def _append_json_line(self, file_path: Path, payload: Mapping[str, object]) -> None:
        ensure_parent_directory(file_path)
        file_path.touch(exist_ok=True)
        with file_path.open("a", encoding="utf-8") as output_file:
            output_file.write(json.dumps(dict(payload), ensure_ascii=False) + "\n")

    def _append_recovery_entry(
        self,
        event_type: str,
        purchase_event_id: str,
        *,
        record: PurchaseRecord | None = None,
    ) -> bool:
        entry: dict[str, object] = {
            "journal_event": event_type,
            "purchase_event_id": purchase_event_id,
            "logged_at": utc_timestamp(),
        }
        if record is not None:
            entry["record"] = record
        try:
            self._append_json_line(self.recovery_file, entry)
            self.logger.info(
                "purchase_sync_recovery_entry_appended event=%s purchase_event_id=%s path=%s",
                event_type,
                purchase_event_id,
                self.recovery_file,
            )
            return True
        except Exception:
            self.logger.exception(
                "purchase_sync_recovery_entry_failed event=%s purchase_event_id=%s path=%s",
                event_type,
                purchase_event_id,
                self.recovery_file,
            )
            return False

    def _load_pending_records_unlocked(self) -> dict[str, PurchaseRecord]:
        pending_records: dict[str, PurchaseRecord] = {}
        ensure_parent_directory(self.recovery_file)
        try:
            self.recovery_file.touch(exist_ok=True)
            lines = self.recovery_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            self.logger.exception(
                "purchase_sync_recovery_read_failed path=%s",
                self.recovery_file,
            )
            return pending_records

        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                self.logger.error(
                    "purchase_sync_recovery_line_invalid path=%s line_number=%s",
                    self.recovery_file,
                    line_number,
                )
                continue

            if not isinstance(payload, dict):
                continue
            journal_event = payload.get("journal_event")
            purchase_event_id = payload.get("purchase_event_id")
            if not isinstance(journal_event, str) or not isinstance(purchase_event_id, str):
                continue

            if journal_event == "pending":
                record = payload.get("record")
                if isinstance(record, dict):
                    pending_records[purchase_event_id] = cast(PurchaseRecord, record)
            elif journal_event == "synced":
                pending_records.pop(purchase_event_id, None)
        return pending_records

    def _queue_purchase_record_unlocked(self, record: PurchaseRecord) -> bool:
        purchase_event_id = record["Purchase Event ID"]
        pending_records = self._load_pending_records_unlocked()
        if purchase_event_id in pending_records:
            return True
        return self._append_recovery_entry("pending", purchase_event_id, record=record)

    def _local_record_exists(self, purchase_event_id: str) -> bool:
        if not self.purchase_log_file.exists():
            return False
        try:
            lines = self.purchase_log_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            self.logger.exception(
                "purchase_log_local_read_failed path=%s",
                self.purchase_log_file,
            )
            return False

        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                self.logger.error(
                    "purchase_log_local_line_invalid path=%s line_number=%s",
                    self.purchase_log_file,
                    line_number,
                )
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("Purchase Event ID") == purchase_event_id:
                return True
        return False

    def _append_local_record(self, record: PurchaseRecord) -> bool:
        purchase_event_id = record["Purchase Event ID"]
        try:
            if self._local_record_exists(purchase_event_id):
                self.logger.info(
                    "purchase_log_local_already_present user_id=%s item_key=%s purchase_event_id=%s path=%s",
                    record["User ID"],
                    record["Item Key"],
                    purchase_event_id,
                    self.purchase_log_file,
                )
                return True

            self._append_json_line(self.purchase_log_file, record)
            self.logger.info(
                "purchase_log_local_appended user_id=%s item_key=%s purchase_event_id=%s timestamp=%s path=%s",
                record["User ID"],
                record["Item Key"],
                purchase_event_id,
                record["Exact Timestamp"],
                self.purchase_log_file,
            )
            return True
        except Exception:
            self.logger.exception(
                "purchase_log_local_append_failed user_id=%s item_key=%s purchase_event_id=%s timestamp=%s path=%s",
                record["User ID"],
                record["Item Key"],
                purchase_event_id,
                record["Exact Timestamp"],
                self.purchase_log_file,
            )
            return False

    def _get_sheet_titles(self, sheets_service: Any) -> dict[str, int]:
        response = (
            sheets_service.spreadsheets()
            .get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
        )
        sheets = cast(list[dict[str, object]], response.get("sheets", []))
        titles: dict[str, int] = {}
        for sheet in sheets:
            properties = sheet.get("properties")
            if not isinstance(properties, dict):
                continue
            title = properties.get("title")
            sheet_id = properties.get("sheetId")
            if isinstance(title, str) and isinstance(sheet_id, int):
                titles[title] = sheet_id
        return titles

    def _ensure_sheet_exists(self, sheets_service: Any, record: PurchaseRecord) -> bool:
        try:
            sheet_titles = self._get_sheet_titles(sheets_service)
        except Exception:
            self.logger.exception(
                "purchase_log_sheet_lookup_failed user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return False

        if self.sheet_tab_name in sheet_titles:
            self.logger.info(
                "purchase_log_sheet_verified user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return True

        try:
            (
                sheets_service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={
                        "requests": [
                            {
                                "addSheet": {
                                    "properties": {"title": self.sheet_tab_name}
                                }
                            }
                        ]
                    },
                )
                .execute()
            )
            self.logger.info(
                "purchase_log_sheet_created user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return True
        except Exception:
            try:
                sheet_titles = self._get_sheet_titles(sheets_service)
            except Exception:
                self.logger.exception(
                    "purchase_log_sheet_create_failed user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                    record["User ID"],
                    record["Item Key"],
                    record["Purchase Event ID"],
                    self.spreadsheet_id,
                    self.sheet_tab_name,
                )
                return False

            if self.sheet_tab_name in sheet_titles:
                self.logger.info(
                    "purchase_log_sheet_verified_after_create_retry user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                    record["User ID"],
                    record["Item Key"],
                    record["Purchase Event ID"],
                    self.spreadsheet_id,
                    self.sheet_tab_name,
                )
                return True

            self.logger.exception(
                "purchase_log_sheet_create_failed user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return False

    def _column_number_to_a1(self, column_number: int) -> str:
        letters = ""
        remaining = column_number
        while remaining > 0:
            remaining, remainder = divmod(remaining - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def _ensure_sheet_header(self, sheets_service: Any, record: PurchaseRecord) -> bool:
        if not self._ensure_sheet_exists(sheets_service, record):
            return False

        try:
            header_response = (
                sheets_service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet_tab_name}!1:1",
                )
                .execute()
            )
            values = cast(list[list[object]], header_response.get("values", []))
            first_row = [str(cell) for cell in values[0]] if values else []
            expected_header = list(PURCHASE_LOG_COLUMNS)
            if first_row == expected_header:
                self.logger.info(
                    "purchase_log_sheet_header_verified user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s",
                    record["User ID"],
                    record["Item Key"],
                    record["Purchase Event ID"],
                    record["Exact Timestamp"],
                    self.spreadsheet_id,
                    self.sheet_tab_name,
                )
                return True

            if first_row and first_row[0] != expected_header[0]:
                self.logger.warning(
                    "purchase_log_sheet_header_preserved_existing_row user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s first_row=%r",
                    record["User ID"],
                    record["Item Key"],
                    record["Purchase Event ID"],
                    record["Exact Timestamp"],
                    self.spreadsheet_id,
                    self.sheet_tab_name,
                    first_row,
                )
                return True

            last_column = self._column_number_to_a1(len(PURCHASE_LOG_COLUMNS))
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
            self.logger.info(
                "purchase_log_sheet_header_written user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                record["Exact Timestamp"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return True
        except Exception:
            self.logger.exception(
                "purchase_log_sheet_header_failed user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                record["Purchase Event ID"],
                record["Exact Timestamp"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return False

    def _sheet_record_exists(self, sheets_service: Any, purchase_event_id: str) -> bool:
        purchase_event_id_column = self._column_number_to_a1(len(PURCHASE_LOG_COLUMNS))
        response = (
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=(
                    f"{self.sheet_tab_name}!{purchase_event_id_column}2:"
                    f"{purchase_event_id_column}"
                ),
            )
            .execute()
        )
        values = cast(list[list[object]], response.get("values", []))
        for row in values:
            if row and str(row[0]).strip() == purchase_event_id:
                return True
        return False

    def _append_sheet_record(self, record: PurchaseRecord) -> bool:
        purchase_event_id = record["Purchase Event ID"]
        sheet_logging_enabled, disabled_reason = self._sheet_logging_state()
        if not sheet_logging_enabled:
            if disabled_reason is not None:
                self._warn_sheet_logging_disabled_once(disabled_reason)
            return True

        try:
            sheets_service = self._get_sheets_service()
        except Exception:
            self.logger.exception(
                "purchase_log_sheet_client_failed user_id=%s item_key=%s purchase_event_id=%s timestamp=%s credentials_path=%s spreadsheet_id=%s",
                record["User ID"],
                record["Item Key"],
                purchase_event_id,
                record["Exact Timestamp"],
                self.credentials_path,
                self.spreadsheet_id,
            )
            return False

        if not self._ensure_sheet_header(sheets_service, record):
            return False

        try:
            if self._sheet_record_exists(sheets_service, purchase_event_id):
                self.logger.info(
                    "purchase_log_sheet_row_already_present user_id=%s item_key=%s purchase_event_id=%s spreadsheet_id=%s sheet=%s",
                    record["User ID"],
                    record["Item Key"],
                    purchase_event_id,
                    self.spreadsheet_id,
                    self.sheet_tab_name,
                )
                return True

            (
                sheets_service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range=self.sheet_tab_name,
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [self._build_sheet_row(record)]},
                )
                .execute()
            )
            self.logger.info(
                "purchase_log_sheet_row_appended user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                purchase_event_id,
                record["Exact Timestamp"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return True
        except Exception:
            self.logger.exception(
                "purchase_log_sheet_row_failed user_id=%s item_key=%s purchase_event_id=%s timestamp=%s spreadsheet_id=%s sheet=%s",
                record["User ID"],
                record["Item Key"],
                purchase_event_id,
                record["Exact Timestamp"],
                self.spreadsheet_id,
                self.sheet_tab_name,
            )
            return False

    def _sync_record_unlocked(self, record: PurchaseRecord) -> tuple[bool, bool]:
        local_ok = self._append_local_record(record)
        sheet_ok = self._append_sheet_record(record)
        if local_ok and sheet_ok:
            self._append_recovery_entry("synced", record["Purchase Event ID"])
        return local_ok, sheet_ok

    def queue_and_sync_purchase(self, record: PurchaseRecord) -> tuple[bool, bool, bool]:
        with self._lock:
            queued_ok = self._queue_purchase_record_unlocked(record)
            if not queued_ok:
                return False, False, False
            local_ok, sheet_ok = self._sync_record_unlocked(record)
            return queued_ok, local_ok, sheet_ok

    def retry_pending_records(self) -> tuple[int, int]:
        with self._lock:
            pending_records = self._load_pending_records_unlocked()
            total_records = len(pending_records)
            synced_records = 0
            for record in pending_records.values():
                local_ok, sheet_ok = self._sync_record_unlocked(record)
                if local_ok and sheet_ok:
                    synced_records += 1
            return synced_records, total_records
