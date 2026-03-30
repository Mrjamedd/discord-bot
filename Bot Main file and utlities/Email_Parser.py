from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


PRIVATE_EMAIL_PARSER_ENV = "DC_BOT_PRIVATE_EMAIL_PARSER_PATH"
DEFAULT_PRIVATE_EMAIL_PARSER_PATH = (
    Path(__file__).resolve().parents[2] / "discord_bot_private" / "Email_Parser_private.py"
)


def _candidate_private_parser_paths() -> tuple[Path, ...]:
    configured_path = (os.getenv(PRIVATE_EMAIL_PARSER_ENV) or "").strip()
    candidates: list[Path] = []
    if configured_path:
        candidates.append(Path(configured_path).expanduser())
    candidates.append(DEFAULT_PRIVATE_EMAIL_PARSER_PATH)

    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        normalized_candidate = candidate.resolve(strict=False)
        if normalized_candidate in seen:
            continue
        seen.add(normalized_candidate)
        unique_candidates.append(normalized_candidate)
    return tuple(unique_candidates)


def _load_private_email_parser() -> ModuleType:
    searched_paths: list[str] = []
    for candidate_path in _candidate_private_parser_paths():
        searched_paths.append(str(candidate_path))
        if not candidate_path.is_file():
            continue

        module_name = "_dc_bot_private_email_parser"
        spec = importlib.util.spec_from_file_location(module_name, candidate_path)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    searched = ", ".join(searched_paths) if searched_paths else "<none>"
    raise ImportError(
        "Private email parser implementation not found. "
        f"Set {PRIVATE_EMAIL_PARSER_ENV} or install Email_Parser_private.py. "
        f"Searched: {searched}"
    )


_PRIVATE_EMAIL_PARSER = _load_private_email_parser()
PAYMENT_PARSER_EXPECTED_AMOUNT = getattr(
    _PRIVATE_EMAIL_PARSER,
    "PAYMENT_PARSER_EXPECTED_AMOUNT",
)
check_payment_email = getattr(
    _PRIVATE_EMAIL_PARSER,
    "check_payment_email",
)


def __getattr__(name: str) -> Any:
    return getattr(_PRIVATE_EMAIL_PARSER, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_PRIVATE_EMAIL_PARSER)))


__all__ = getattr(
    _PRIVATE_EMAIL_PARSER,
    "__all__",
    ["PAYMENT_PARSER_EXPECTED_AMOUNT", "check_payment_email"],
)


class _PrivateEmailParserProxyModule(ModuleType):
    _LOCAL_ONLY_NAMES = frozenset(
        {
            "PRIVATE_EMAIL_PARSER_ENV",
            "DEFAULT_PRIVATE_EMAIL_PARSER_PATH",
            "_candidate_private_parser_paths",
            "_load_private_email_parser",
            "_PRIVATE_EMAIL_PARSER",
            "_PrivateEmailParserProxyModule",
            "__all__",
            "__spec__",
            "__loader__",
            "__package__",
            "__file__",
            "__cached__",
            "__builtins__",
        }
    )

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name not in self._LOCAL_ONLY_NAMES:
            setattr(_PRIVATE_EMAIL_PARSER, name, value)

    def __delattr__(self, name: str) -> None:
        had_local_attr = hasattr(self, name)
        if had_local_attr:
            super().__delattr__(name)
        if name not in self._LOCAL_ONLY_NAMES and hasattr(_PRIVATE_EMAIL_PARSER, name):
            delattr(_PRIVATE_EMAIL_PARSER, name)


sys.modules[__name__].__class__ = _PrivateEmailParserProxyModule
