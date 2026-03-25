from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from config import (
    CONSUMED_MESSAGE_ID_RETENTION_DAYS,
    STATE_FILE,
)
from models import BotState, ParserReplayState, TicketRecord
from ticketing import (
    PAYMENT_PLATFORMS_BY_KEY,
    SCRIPT_PRODUCTS_BY_KEY,
    TICKET_STAGE_AWAITING_SELECTION,
    VALID_TICKET_STAGES,
    resolve_script_product_key,
)
from utils import ensure_parent_directory


def fresh_payment_parser_state() -> ParserReplayState:
    return {"consumed_message_ids": {}}


def fresh_ticket_record(owner_id: int | None = None) -> TicketRecord:
    record: TicketRecord = {
        "selected_script_key": None,
        "payment_platform_key": None,
        "payment_note_code": None,
        "stage": TICKET_STAGE_AWAITING_SELECTION,
    }
    if owner_id is not None:
        record["owner_id"] = owner_id
    return record


def _coerce_consumed_message_ids(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}

    consumed_message_ids: dict[str, str] = {}
    for message_id, consumed_at in value.items():
        if not isinstance(message_id, str) or not isinstance(consumed_at, str):
            continue
        try:
            datetime.fromisoformat(consumed_at)
        except ValueError:
            continue
        consumed_message_ids[message_id] = consumed_at
    return consumed_message_ids


def _coerce_payment_parser_state(value: object) -> ParserReplayState:
    if not isinstance(value, dict):
        return fresh_payment_parser_state()

    return {
        "consumed_message_ids": _coerce_consumed_message_ids(
            value.get("consumed_message_ids")
        )
    }


def _coerce_ticket_record(value: object) -> TicketRecord:
    if not isinstance(value, dict):
        return fresh_ticket_record()

    owner_id_value = value.get("owner_id")
    selected_script_key_value = value.get("selected_script_key")
    payment_platform_key_value = value.get("payment_platform_key")
    payment_note_code_value = value.get("payment_note_code")
    stage_value = value.get("stage")

    record = fresh_ticket_record()
    if isinstance(owner_id_value, int) and not isinstance(owner_id_value, bool):
        record["owner_id"] = owner_id_value
    if isinstance(selected_script_key_value, str):
        resolved_script_key = resolve_script_product_key(selected_script_key_value)
        if resolved_script_key in SCRIPT_PRODUCTS_BY_KEY:
            record["selected_script_key"] = resolved_script_key
    if isinstance(payment_platform_key_value, str):
        if payment_platform_key_value in PAYMENT_PLATFORMS_BY_KEY:
            record["payment_platform_key"] = payment_platform_key_value
    if isinstance(payment_note_code_value, str):
        normalized_note_code = payment_note_code_value.strip().upper()
        if normalized_note_code:
            record["payment_note_code"] = normalized_note_code
    if isinstance(stage_value, str) and stage_value in VALID_TICKET_STAGES:
        record["stage"] = stage_value
    return record


def _coerce_state(value: object) -> BotState:
    state: BotState = {
        "tickets": {},
        "payment_parser": fresh_payment_parser_state(),
    }
    if not isinstance(value, dict):
        return state

    raw_tickets = value.get("tickets")
    if isinstance(raw_tickets, dict):
        state["tickets"] = {
            channel_id: _coerce_ticket_record(record)
            for channel_id, record in raw_tickets.items()
            if isinstance(channel_id, str)
        }

    state["payment_parser"] = _coerce_payment_parser_state(value.get("payment_parser"))

    ticket_panel_message_id = value.get("ticket_panel_message_id")
    if isinstance(ticket_panel_message_id, int) and not isinstance(
        ticket_panel_message_id,
        bool,
    ):
        state["ticket_panel_message_id"] = ticket_panel_message_id

    support_ticket_panel_message_id = value.get("support_ticket_panel_message_id")
    if isinstance(support_ticket_panel_message_id, int) and not isinstance(
        support_ticket_panel_message_id,
        bool,
    ):
        state["support_ticket_panel_message_id"] = support_ticket_panel_message_id

    return state


def load_state() -> BotState:
    if not STATE_FILE.exists():
        return {
            "tickets": {},
            "payment_parser": fresh_payment_parser_state(),
        }

    try:
        raw_state: object = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {
            "tickets": {},
            "payment_parser": fresh_payment_parser_state(),
        }
    return _coerce_state(raw_state)


def save_state(state: BotState) -> None:
    serialized = json.dumps(state, indent=2)
    ensure_parent_directory(STATE_FILE)
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(serialized, encoding="utf-8")
    temp_file.replace(STATE_FILE)


def get_ticket_record(
    state: BotState,
    channel_id: str,
    *,
    owner_id: int | None = None,
) -> TicketRecord:
    tickets = state.get("tickets")
    if tickets is None:
        tickets = {}
        state["tickets"] = tickets

    record = tickets.get(channel_id)
    if record is None:
        record = fresh_ticket_record(owner_id)
        tickets[channel_id] = record
        return record

    if owner_id is not None and record.get("owner_id") is None:
        record["owner_id"] = owner_id
    return record


def get_payment_parser_state(state: BotState) -> ParserReplayState:
    parser_state = state.get("payment_parser")
    if not isinstance(parser_state, dict):
        parser_state = fresh_payment_parser_state()
        state["payment_parser"] = parser_state
    return parser_state


def purge_consumed_message_ids(
    parser_state: ParserReplayState,
    *,
    now_utc: datetime | None = None,
) -> bool:
    current_time = now_utc or datetime.now(timezone.utc)
    retention_cutoff = current_time - timedelta(days=CONSUMED_MESSAGE_ID_RETENTION_DAYS)
    consumed_message_ids = parser_state["consumed_message_ids"]

    expired_message_ids = [
        message_id
        for message_id, consumed_at in consumed_message_ids.items()
        if _consumed_at_is_expired(consumed_at, retention_cutoff)
    ]
    for message_id in expired_message_ids:
        consumed_message_ids.pop(message_id, None)
    return bool(expired_message_ids)


def _consumed_at_is_expired(consumed_at: str, retention_cutoff: datetime) -> bool:
    try:
        consumed_at_dt = datetime.fromisoformat(consumed_at)
    except ValueError:
        return True

    if consumed_at_dt.tzinfo is None:
        consumed_at_dt = consumed_at_dt.replace(tzinfo=timezone.utc)
    else:
        consumed_at_dt = consumed_at_dt.astimezone(timezone.utc)
    return consumed_at_dt < retention_cutoff


def record_consumed_message_id(
    parser_state: ParserReplayState,
    gmail_message_id: str,
    *,
    consumed_at_utc: datetime | None = None,
) -> None:
    timestamp = consumed_at_utc or datetime.now(timezone.utc)
    parser_state["consumed_message_ids"][gmail_message_id] = timestamp.isoformat()
