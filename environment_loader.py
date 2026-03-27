from __future__ import annotations

import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DOTENV_PATH = REPO_ROOT / ".env"
DEFAULT_HOME_DOTENV_PATH = Path.home() / ".env"
_DOTENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BOT_DOTENV_HINT_KEYS = frozenset(
    {
        "DISCORD_BOT_TOKEN",
        "TICKET_PANEL_CHANNEL_ID",
        "PAYMENT_PARSER_GMAIL_ADDRESS",
    }
)


def _parse_dotenv_line(raw_line: str) -> tuple[str, str] | None:
    stripped = raw_line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if stripped.startswith("export "):
        stripped = stripped[7:].lstrip()

    key, separator, value = stripped.partition("=")
    if not separator:
        return None

    normalized_key = key.strip()
    if not _DOTENV_KEY_PATTERN.fullmatch(normalized_key):
        return None

    normalized_value = value.strip()
    if len(normalized_value) >= 2 and normalized_value[0] == normalized_value[-1]:
        if normalized_value[0] in {'"', "'"}:
            normalized_value = normalized_value[1:-1]

    return normalized_key, normalized_value


def _resolve_repo_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def _candidate_dotenv_paths(dotenv_path: Path | None = None) -> tuple[tuple[Path, bool], ...]:
    if dotenv_path is not None:
        return ((_resolve_repo_path(dotenv_path), True),)

    env_override = (os.getenv("DC_BOT_ENV_FILE") or "").strip()
    if env_override:
        return ((_resolve_repo_path(env_override), True),)

    candidates: list[tuple[Path, bool]] = [(DEFAULT_DOTENV_PATH, True)]
    if DEFAULT_HOME_DOTENV_PATH != DEFAULT_DOTENV_PATH:
        candidates.append((DEFAULT_HOME_DOTENV_PATH, False))
    return tuple(candidates)


def _looks_like_bot_dotenv(dotenv_path: Path) -> bool:
    try:
        raw_lines = dotenv_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False

    for raw_line in raw_lines:
        parsed = _parse_dotenv_line(raw_line)
        if parsed is None:
            continue
        key, _ = parsed
        if key in _BOT_DOTENV_HINT_KEYS:
            return True
    return False


def load_dotenv_if_present(dotenv_path: Path | None = None) -> bool:
    for resolved_path, is_explicit in _candidate_dotenv_paths(dotenv_path):
        if not resolved_path.is_file():
            continue
        if not is_explicit and not _looks_like_bot_dotenv(resolved_path):
            continue

        for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_dotenv_line(raw_line)
            if parsed is None:
                continue
            key, value = parsed
            os.environ.setdefault(key, value)
        return True
    return False
