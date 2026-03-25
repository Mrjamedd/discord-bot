from __future__ import annotations

import base64
import binascii
import io
import os
import re
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DOTENV_PATH = REPO_ROOT / ".env"
DEFAULT_HOME_DOTENV_PATH = Path.home() / ".env"
DEFAULT_BUNDLED_GPC_DIR = REPO_ROOT / "Bot Main file and utlities" / "gpc_files"
GPC_ARCHIVE_ENV = "DC_BOT_GPC_ARCHIVE_ZIP_BASE64"
REQUIRED_GPC_FILENAMES: tuple[str, ...] = (
    "Corex-Aim_2K26.gpc",
    "GOLDEN_FREE_v2.gpc",
    "secretofscript(unrealeased) (1).gpc",
    "SWOOSH V2.gpc",
)
_DOTENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BOT_DOTENV_HINT_KEYS = frozenset(
    {
        "DISCORD_BOT_TOKEN",
        "TICKET_PANEL_CHANNEL_ID",
        "PAYMENT_PARSER_GMAIL_ADDRESS",
        GPC_ARCHIVE_ENV,
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
        # OCI/Ubuntu deployments often keep the live bot env outside the repo.
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


def _extract_required_gpc_files(archive_bytes: bytes, target_dir: Path) -> Path:
    try:
        archive = ZipFile(io.BytesIO(archive_bytes))
    except BadZipFile as exc:
        raise RuntimeError(
            f"{GPC_ARCHIVE_ENV} is not a valid base64-encoded zip archive."
        ) from exc

    with archive:
        matched_members: dict[str, str] = {}
        for archive_member in archive.infolist():
            if archive_member.is_dir():
                continue
            basename = PurePosixPath(archive_member.filename).name
            if basename not in REQUIRED_GPC_FILENAMES:
                continue
            if basename in matched_members:
                raise RuntimeError(
                    f"{GPC_ARCHIVE_ENV} contains duplicate copies of {basename!r}."
                )
            matched_members[basename] = archive_member.filename

        missing_filenames = sorted(
            filename
            for filename in REQUIRED_GPC_FILENAMES
            if filename not in matched_members
        )
        if missing_filenames:
            raise RuntimeError(
                f"{GPC_ARCHIVE_ENV} is missing required delivery files: "
                f"{', '.join(missing_filenames)}."
            )

        target_dir.mkdir(parents=True, exist_ok=True)
        for basename, archive_member_name in matched_members.items():
            destination = target_dir / basename
            destination.write_bytes(archive.read(archive_member_name))
    return target_dir


def _default_provisioned_gpc_dir() -> Path:
    runtime_dir_value = os.getenv("DC_BOT_RUNTIME_DIR", "runtime")
    if not runtime_dir_value.strip():
        runtime_dir_value = "runtime"
    return _resolve_repo_path(runtime_dir_value) / "private_gpc"


def _provisioned_script_files_dir() -> Path:
    raw_value = os.getenv("SCRIPT_FILES_DIR")
    if raw_value is None or not raw_value.strip():
        return _default_provisioned_gpc_dir()

    configured_dir = _resolve_repo_path(raw_value)
    if configured_dir == DEFAULT_BUNDLED_GPC_DIR:
        return _default_provisioned_gpc_dir()
    return configured_dir


def provision_private_gpc_assets() -> Path | None:
    raw_archive = os.getenv(GPC_ARCHIVE_ENV, "")
    if not raw_archive.strip():
        return None
    encoded_archive = "".join(raw_archive.split())

    try:
        archive_bytes = base64.b64decode(encoded_archive, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            f"{GPC_ARCHIVE_ENV} must be valid base64."
        ) from exc

    script_files_dir = _provisioned_script_files_dir()
    extracted_dir = _extract_required_gpc_files(archive_bytes, script_files_dir)
    os.environ["SCRIPT_FILES_DIR"] = str(extracted_dir)
    return extracted_dir


def bootstrap_environment() -> None:
    load_dotenv_if_present()
    provision_private_gpc_assets()
