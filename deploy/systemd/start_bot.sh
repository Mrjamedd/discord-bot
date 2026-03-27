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

cd "$REPO_ROOT"

if [[ ! -d "$REPO_ROOT/.git" ]]; then
  log "Missing git repository at $REPO_ROOT"
  exit 1
fi

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

if ! find "$ASSET_DIR" -mindepth 1 -maxdepth 1 -type f -print -quit | grep -q .; then
  log "Required asset directory is empty: $ASSET_DIR"
  exit 1
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "$BOT_GIT_BRANCH" ]]; then
  log "Expected branch '$BOT_GIT_BRANCH' but found '$current_branch'"
  exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  log "Refusing to auto-update because the repository is not clean. Tracked edits or unignored local files are present."
  git status --short
  exit 1
fi

local_head="$(git rev-parse HEAD)"
log "Fetching $BOT_GIT_REMOTE/$BOT_GIT_BRANCH"
git fetch --prune "$BOT_GIT_REMOTE" "$BOT_GIT_BRANCH"
remote_head="$(git rev-parse FETCH_HEAD)"

requirements_changed=0
if [[ "$local_head" != "$remote_head" ]]; then
  log "Fast-forwarding from $local_head to $remote_head"
  git merge --ff-only FETCH_HEAD
  if ! git diff --quiet "$local_head" HEAD -- requirements.txt; then
    requirements_changed=1
  fi
else
  log "Repository already up to date on $BOT_GIT_BRANCH"
fi

current_head="$(git rev-parse HEAD)"
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
