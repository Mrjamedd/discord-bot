from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict


class TicketRecord(TypedDict, total=False):
    owner_id: int
    selected_script_key: str | None
    ticket_price_override: str | None
    payment_platform_key: str | None
    payment_note_code: str | None
    auto_close_at_utc: str | None
    stage: str


class ParserReplayState(TypedDict):
    consumed_message_ids: dict[str, str]


class BotState(TypedDict, total=False):
    ticket_panel_message_id: int
    support_ticket_panel_message_id: int
    tickets: dict[str, TicketRecord]
    payment_parser: ParserReplayState


class PaymentParserResult(TypedDict, total=False):
    matched: bool
    reason: str
    gmail_message_id: str | None
    from_address: str | None
    from_domain: str | None
    allowed_sender_domains: list[str]
    allowed_sender_subdomains: list[str]
    amount: str | None
    expected_amount: str | None
    amount_shortfall: str | None
    expected_payment_note: str | None
    currency: str | None
    received_timestamp_utc: str | None
    auth_summary: str
    forwarding_flags: list[str]
    amount_candidates: list[str]
    weak_forwarding_flags: list[str]
    timestamp_in_window: bool
    auth_strength: int
    sender_address_allowlisted: bool
    expected_payment_note_found: bool


PurchaseRecord = TypedDict(
    "PurchaseRecord",
    {
        "Full Date": str,
        "Exact Timestamp": str,
        "Discord Username": str,
        "Display Name": str,
        "User ID": int,
        "Item Purchased": str,
        "Item Key": str,
        "Delivered File": str,
        "Price Paid": str,
        "Payment Method": str,
        "Payment Method Key": str,
        "Channel ID": int,
        "Guild ID": int,
        "Purchase Event ID": str,
    },
    total=False,
)

PURCHASE_LOG_COLUMNS: tuple[str, ...] = (
    "Full Date",
    "Exact Timestamp",
    "Discord Username",
    "Display Name",
    "User ID",
    "Item Purchased",
    "Item Key",
    "Delivered File",
    "Price Paid",
    "Channel ID",
    "Guild ID",
    "Purchase Event ID",
)


@dataclass(frozen=True)
class ScriptProduct:
    key: str
    label: str
    price: int
    file_path: Path
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class PaymentPlatform:
    key: str
    label: str
    destination_label: str
    destination_value: str
