from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_DIR = REPO_ROOT / "Bot Main file and utlities"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

import Email_Parser as email_parser


class EmailParserWrapperTests(unittest.TestCase):
    def tearDown(self) -> None:
        email_parser._PRIVATE_EMAIL_PARSER = None
        email_parser._PRIVATE_EMAIL_PARSER_LOAD_ERROR = None

    def test_private_email_parser_config_error_uses_bundled_parser_when_legacy_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bundled_parser_path = Path(temp_dir) / "bundled_email_parser.py"
            bundled_parser_path.write_text(
                "def check_payment_email(*args, **kwargs):\n"
                "    return {'matched': False, 'reason': 'stub'}\n",
                encoding="utf-8",
            )

            with (
                patch.object(email_parser, "_BUNDLED_PRIVATE_EMAIL_PARSER_PATH", bundled_parser_path),
                patch.object(
                    email_parser,
                    "_LEGACY_PRIVATE_EMAIL_PARSER_PATH",
                    Path(temp_dir) / "missing_legacy_parser.py",
                ),
            ):
                self.assertIsNone(email_parser.private_email_parser_config_error())

    def test_check_payment_email_normalizes_unmatched_note_rejection(self) -> None:
        parser_module = SimpleNamespace(
            check_payment_email=lambda **_: {
                "matched": False,
                "reason": "payment note missing",
                "gmail_message_id": "old-receipt",
                "from_address": "no-reply@cash.app",
                "from_domain": "cash.app",
                "amount": "23.00",
                "currency": "USD",
            }
        )

        with patch.object(email_parser, "_get_private_email_parser", return_value=parser_module):
            result = email_parser.check_payment_email(
                expected_payment_note="ticket-code-123",
            )

        self.assertFalse(result["matched"])
        self.assertEqual(result["reason"], "no candidate messages found")
        self.assertIsNone(result["gmail_message_id"])
        self.assertEqual(result["expected_payment_note"], "TICKET-CODE-123")
        self.assertFalse(result["expected_payment_note_found"])

    def test_check_payment_email_keeps_relevant_rejection_when_note_was_found(self) -> None:
        parser_module = SimpleNamespace(
            check_payment_email=lambda **_: {
                "matched": False,
                "reason": "amount short",
                "gmail_message_id": "real-receipt",
                "expected_payment_note_found": True,
                "amount_shortfall": "5.00",
            }
        )

        with patch.object(email_parser, "_get_private_email_parser", return_value=parser_module):
            result = email_parser.check_payment_email(
                expected_payment_note="ticket-code-123",
            )

        self.assertFalse(result["matched"])
        self.assertEqual(result["reason"], "amount short")
        self.assertEqual(result["gmail_message_id"], "real-receipt")
        self.assertEqual(result["amount_shortfall"], "5.00")


if __name__ == "__main__":
    unittest.main()
