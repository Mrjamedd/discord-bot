from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

import state_manager
from state_manager import (
    _coerce_ticket_record,
    fresh_ticket_record,
    load_state_result,
    save_state,
)


class TicketStateManagerTests(unittest.TestCase):
    def test_fresh_ticket_record_starts_without_auto_close_deadline(self) -> None:
        record = fresh_ticket_record(owner_id=123)

        self.assertEqual(123, record["owner_id"])
        self.assertIsNone(record["auto_close_at_utc"])
        self.assertIsNone(record["ticket_price_override"])

    def test_coerce_ticket_record_keeps_valid_auto_close_deadline(self) -> None:
        record = _coerce_ticket_record(
            {
                "owner_id": 123,
                "stage": "completed",
                "auto_close_at_utc": "2026-03-31T12:34:56+00:00",
            }
        )

        self.assertEqual("completed", record["stage"])
        self.assertEqual("2026-03-31T12:34:56+00:00", record["auto_close_at_utc"])

    def test_coerce_ticket_record_discards_invalid_auto_close_deadline(self) -> None:
        record = _coerce_ticket_record(
            {
                "stage": "completed",
                "auto_close_at_utc": "not-a-real-timestamp",
            }
        )

        self.assertIsNone(record["auto_close_at_utc"])

    def test_coerce_ticket_record_keeps_valid_ticket_price_override(self) -> None:
        record = _coerce_ticket_record(
            {
                "ticket_price_override": "$15.50",
            }
        )

        self.assertEqual("15.50", record["ticket_price_override"])

    def test_coerce_ticket_record_discards_invalid_ticket_price_override(self) -> None:
        record = _coerce_ticket_record(
            {
                "ticket_price_override": "-4",
            }
        )

        self.assertIsNone(record["ticket_price_override"])

    def test_load_state_result_falls_back_to_backup_when_primary_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "state.json"
            backup_file = Path(temp_dir) / "state.backup.json"
            state_file.write_text("{not valid json", encoding="utf-8")
            backup_file.write_text(
                json.dumps(
                    {
                        "tickets": {
                            "123": {
                                "owner_id": 99,
                                "stage": "awaiting_selection",
                            }
                        },
                        "payment_parser": {"consumed_message_ids": {}},
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(state_manager, "STATE_FILE", state_file), patch.object(
                state_manager,
                "STATE_BACKUP_FILE",
                backup_file,
            ):
                result = load_state_result()
                archived_files = list(Path(temp_dir).glob("state.corrupt-*.json"))

        self.assertEqual("backup", result.source)
        self.assertEqual(99, result.state["tickets"]["123"]["owner_id"])
        self.assertTrue(
            any("Recovered bot state from backup file" in warning for warning in result.warnings)
        )
        self.assertEqual(1, len(archived_files))

    def test_save_state_writes_primary_and_backup_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "state.json"
            backup_file = Path(temp_dir) / "state.backup.json"
            state = {
                "tickets": {
                    "123": {
                        "owner_id": 99,
                        "stage": "awaiting_selection",
                    }
                },
                "payment_parser": {"consumed_message_ids": {}},
            }

            with patch.object(state_manager, "STATE_FILE", state_file), patch.object(
                state_manager,
                "STATE_BACKUP_FILE",
                backup_file,
            ):
                save_state(state)

            self.assertEqual(
                json.loads(state_file.read_text(encoding="utf-8")),
                json.loads(backup_file.read_text(encoding="utf-8")),
            )


if __name__ == "__main__":
    unittest.main()
