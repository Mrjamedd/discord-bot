from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import discord

from config import (
    DISCORD_MESSAGE_LIMIT,
)
from sheets_error_logger import GoogleSheetsErrorHandler


def ensure_parent_directory(file_path: str | os.PathLike[str]) -> None:
    Path(file_path).expanduser().parent.mkdir(parents=True, exist_ok=True)


def _remove_local_file_handlers(logger: logging.Logger) -> None:
    for handler in tuple(logger.handlers):
        if not isinstance(handler, RotatingFileHandler):
            continue
        logger.removeHandler(handler)
        handler.close()


def setup_payment_parser_logger() -> logging.Logger:
    parser_logger = logging.getLogger("dc_bot.payment_parser")
    parser_logger.setLevel(logging.INFO)
    _remove_local_file_handlers(parser_logger)
    parser_logger.propagate = True
    return parser_logger


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("dc_bot")
    logger.setLevel(logging.INFO)
    _remove_local_file_handlers(logger)

    if not any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, GoogleSheetsErrorHandler)
        for handler in logger.handlers
    ):
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if not any(
        isinstance(handler, GoogleSheetsErrorHandler)
        for handler in logger.handlers
    ):
        logger.addHandler(GoogleSheetsErrorHandler())

    logger.propagate = False
    setup_payment_parser_logger()
    return logger


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return re.sub(r"[^a-z0-9 ]+", "", lowered)



def message_has_component_custom_id(message: discord.Message, custom_id: str) -> bool:
    for action_row in message.components:
        for component in getattr(action_row, "children", ()):
            if getattr(component, "custom_id", None) == custom_id:
                return True
    return False


def split_message(text: str, limit: int = DISCORD_MESSAGE_LIMIT) -> list[str]:
    cleaned = text.strip() or "No response returned. Please try again."
    chunks: list[str] = []
    remaining = cleaned

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at <= 0:
            split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at <= 0:
            split_at = limit

        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()

    return chunks

def build_channel_name(username: str, *, prefix: str) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", username.lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = "user"
    max_slug_length = max(1, 99 - len(prefix))
    return f"{prefix}-{slug[:max_slug_length]}"
