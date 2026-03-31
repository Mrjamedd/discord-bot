from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from state_manager import _coerce_ticket_record, fresh_ticket_record


class TicketStateManagerTests(unittest.TestCase):
    def test_fresh_ticket_record_starts_without_auto_close_deadline(self) -> None:
        record = fresh_ticket_record(owner_id=123)

        self.assertEqual(123, record["owner_id"])
        self.assertIsNone(record["auto_close_at_utc"])

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


if __name__ == "__main__":
    unittest.main()
