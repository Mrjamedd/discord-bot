#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"

BOT_GIT_REMOTE="${BOT_GIT_REMOTE:-origin}"
BOT_GIT_BRANCH="${BOT_GIT_BRANCH:-main}"
BOT_AUTO_PIP_INSTALL="${BOT_AUTO_PIP_INSTALL:-1}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/venv/bin/python}"
PIP_BIN="${PIP_BIN:-}"
RUNNER_PATH="${RUNNER_PATH:-$REPO_ROOT/run_bot.py}"
ASSET_DIR="/home/ubuntu/discord-bot/assets"

log() {
  printf '[start_bot] %s\n' "$*"
}

warn() {
  printf '[start_bot] WARNING: %s\n' "$*" >&2
}

cd "$REPO_ROOT"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log "Python executable not found: $PYTHON_BIN"
  exit 1
fi

if [[ ! -f "$RUNNER_PATH" ]]; then
  log "Bot launcher not found: $RUNNER_PATH"
  exit 1
fi

if [[ ! -e "$ASSET_DIR" ]]; then
  log "Required asset directory is missing: $ASSET_DIR"
  exit 1
fi

if [[ ! -d "$ASSET_DIR" ]]; then
  log "Required asset path is not a directory: $ASSET_DIR"
  exit 1
fi

if [[ ! -r "$ASSET_DIR" || ! -x "$ASSET_DIR" ]]; then
  log "Required asset directory is unreadable: $ASSET_DIR"
  exit 1
fi

if ! find "$ASSET_DIR" -mindepth 1 -maxdepth 1 -type f -iname '*.gpc' -print -quit | grep -q .; then
  log "Required asset directory does not contain any delivery .gpc files: $ASSET_DIR"
  exit 1
fi

requirements_changed=0
current_head="unknown"
auto_update_enabled=1

if [[ ! -d "$REPO_ROOT/.git" ]]; then
  warn "Missing git repository at $REPO_ROOT; skipping automatic update and starting current files."
  auto_update_enabled=0
fi

if (( auto_update_enabled )); then
  current_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [[ -z "$current_branch" ]]; then
    warn "Unable to determine the current git branch; skipping automatic update."
    auto_update_enabled=0
  elif [[ "$current_branch" != "$BOT_GIT_BRANCH" ]]; then
    warn "Expected branch '$BOT_GIT_BRANCH' but found '$current_branch'; skipping automatic update."
    auto_update_enabled=0
  fi
fi

if (( auto_update_enabled )); then
  if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
    warn "Repository is not clean; skipping automatic update and starting current revision."
    git status --short || true
    auto_update_enabled=0
  fi
fi

if (( auto_update_enabled )); then
  local_head="$(git rev-parse HEAD)"
  log "Fetching $BOT_GIT_REMOTE/$BOT_GIT_BRANCH"
  if git fetch --prune "$BOT_GIT_REMOTE" "$BOT_GIT_BRANCH"; then
    remote_head="$(git rev-parse FETCH_HEAD)"
    if [[ "$local_head" != "$remote_head" ]]; then
      log "Fast-forwarding from $local_head to $remote_head"
      if git merge --ff-only FETCH_HEAD; then
        if ! git diff --quiet "$local_head" HEAD -- requirements.txt; then
          requirements_changed=1
        fi
      else
        warn "Fast-forward merge failed; starting the current local revision instead."
      fi
    else
      log "Repository already up to date on $BOT_GIT_BRANCH"
    fi
  else
    warn "git fetch failed; starting the current local revision instead."
  fi
fi

current_head="$(git rev-parse HEAD 2>/dev/null || printf 'unknown')"
log "Repository ready at commit $current_head"

if [[ "$BOT_AUTO_PIP_INSTALL" == "1" ]]; then
  if (( requirements_changed )); then
    if [[ -n "$PIP_BIN" ]]; then
      if [[ ! -x "$PIP_BIN" ]]; then
        log "pip executable not found: $PIP_BIN"
        exit 1
      fi
      log "requirements.txt changed; installing dependencies with $PIP_BIN"
      "$PIP_BIN" install -r "$REPO_ROOT/requirements.txt"
    else
      log "requirements.txt changed; installing dependencies with $PYTHON_BIN -m pip"
      "$PYTHON_BIN" -m pip install -r "$REPO_ROOT/requirements.txt"
    fi
  else
    log "requirements.txt unchanged; skipping pip install"
  fi
else
  log "BOT_AUTO_PIP_INSTALL disabled; skipping pip install"
fi

log "Starting bot"
exec "$PYTHON_BIN" "$RUNNER_PATH"
