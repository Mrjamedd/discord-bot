from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import (
    CONSUMED_MESSAGE_ID_RETENTION_DAYS,
    STATE_BACKUP_FILE,
    STATE_FILE,
)
from models import BotState, ParserReplayState, TicketRecord
from ticketing import (
    PAYMENT_PLATFORMS_BY_KEY,
    SCRIPT_PRODUCTS_BY_KEY,
    TICKET_STAGE_AWAITING_SELECTION,
    VALID_TICKET_STAGES,
    normalize_ticket_price_text,
    resolve_script_product_key,
)
from utils import ensure_parent_directory


@dataclass(frozen=True)
class StateLoadResult:
    state: BotState
    source: str
    warnings: tuple[str, ...] = ()


def fresh_payment_parser_state() -> ParserReplayState:
    return {"consumed_message_ids": {}}


def fresh_ticket_record(owner_id: int | None = None) -> TicketRecord:
    record: TicketRecord = {
        "selected_script_key": None,
        "ticket_price_override": None,
        "payment_platform_key": None,
        "payment_note_code": None,
        "auto_close_at_utc": None,
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
    ticket_price_override_value = value.get("ticket_price_override")
    payment_platform_key_value = value.get("payment_platform_key")
    payment_note_code_value = value.get("payment_note_code")
    auto_close_at_utc_value = value.get("auto_close_at_utc")
    stage_value = value.get("stage")

    record = fresh_ticket_record()
    if isinstance(owner_id_value, int) and not isinstance(owner_id_value, bool):
        record["owner_id"] = owner_id_value
    if isinstance(selected_script_key_value, str):
        resolved_script_key = resolve_script_product_key(selected_script_key_value)
        if resolved_script_key in SCRIPT_PRODUCTS_BY_KEY:
            record["selected_script_key"] = resolved_script_key
    if isinstance(ticket_price_override_value, str):
        normalized_price_override = normalize_ticket_price_text(
            ticket_price_override_value
        )
        if normalized_price_override is not None:
            record["ticket_price_override"] = normalized_price_override
    if isinstance(payment_platform_key_value, str):
        if payment_platform_key_value in PAYMENT_PLATFORMS_BY_KEY:
            record["payment_platform_key"] = payment_platform_key_value
    if isinstance(payment_note_code_value, str):
        normalized_note_code = payment_note_code_value.strip().upper()
        if normalized_note_code:
            record["payment_note_code"] = normalized_note_code
    if isinstance(auto_close_at_utc_value, str):
        normalized_auto_close_at_utc = auto_close_at_utc_value.strip()
        if normalized_auto_close_at_utc:
            try:
                datetime.fromisoformat(normalized_auto_close_at_utc)
            except ValueError:
                pass
            else:
                record["auto_close_at_utc"] = normalized_auto_close_at_utc
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


def _default_state() -> BotState:
    return {
        "tickets": {},
        "payment_parser": fresh_payment_parser_state(),
    }


def _archive_invalid_state_file(state_file: Path) -> str | None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_name = (
        f"{state_file.stem}.corrupt-{timestamp}{state_file.suffix or '.json'}"
    )
    archive_path = state_file.with_name(archive_name)
    try:
        state_file.replace(archive_path)
    except OSError as exc:
        return (
            f"Failed to archive unreadable state file {state_file} to {archive_path}: {exc}"
        )
    return f"Archived unreadable state file to {archive_path}"


def _load_state_file(state_file: Path) -> BotState:
    raw_state: object = json.loads(state_file.read_text(encoding="utf-8"))
    return _coerce_state(raw_state)


def load_state_result() -> StateLoadResult:
    warnings: list[str] = []

    for source_name, state_file in (
        ("primary", STATE_FILE),
        ("backup", STATE_BACKUP_FILE),
    ):
        if not state_file.exists():
            continue

        try:
            state = _load_state_file(state_file)
        except json.JSONDecodeError as exc:
            warnings.append(f"State file {state_file} is invalid JSON: {exc}")
            if source_name == "primary":
                archive_warning = _archive_invalid_state_file(state_file)
                if archive_warning is not None:
                    warnings.append(archive_warning)
            continue
        except OSError as exc:
            warnings.append(f"State file {state_file} could not be read: {exc}")
            continue

        if source_name == "backup":
            warnings.append(f"Recovered bot state from backup file {state_file}.")
        return StateLoadResult(
            state=state,
            source=source_name,
            warnings=tuple(warnings),
        )

    return StateLoadResult(
        state=_default_state(),
        source="fresh",
        warnings=tuple(warnings),
    )


def load_state() -> BotState:
    return load_state_result().state


def _fsync_directory(directory: Path) -> None:
    try:
        directory_fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_atomic_text(file_path: Path, content: str) -> None:
    ensure_parent_directory(file_path)
    temp_file = file_path.with_name(f"{file_path.name}.tmp")
    with temp_file.open("w", encoding="utf-8") as output_file:
        output_file.write(content)
        output_file.flush()
        os.fsync(output_file.fileno())
    temp_file.replace(file_path)
    _fsync_directory(file_path.parent)


def save_state(state: BotState) -> None:
    serialized = json.dumps(state, indent=2, sort_keys=True)
    _write_atomic_text(STATE_FILE, serialized)
    try:
        _write_atomic_text(STATE_BACKUP_FILE, serialized)
    except OSError:
        # The primary state write already succeeded. Keep the backup as
        # best-effort so transient backup failures do not block the bot.
        return


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
