#!/usr/bin/env bash

set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-discord-purchase-bot}"
SERVICE_USER="${SERVICE_USER:-ubuntu}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"

INSTALL_DIR="${INSTALL_DIR:-$REPO_ROOT}"
ENV_FILE="${ENV_FILE:-/home/$SERVICE_USER/.env}"
GIT_REMOTE="${GIT_REMOTE:-origin}"
GIT_BRANCH="${GIT_BRANCH:-main}"
AUTO_PIP_INSTALL="${AUTO_PIP_INSTALL:-1}"
PYTHON_BIN="${PYTHON_BIN:-$INSTALL_DIR/venv/bin/python}"
RUNNER_PATH="${RUNNER_PATH:-$INSTALL_DIR/run_bot.py}"
START_SCRIPT_PATH="${START_SCRIPT_PATH:-$INSTALL_DIR/deploy/systemd/start_bot.sh}"
UNIT_SOURCE="$SCRIPT_DIR/discord-purchase-bot.service"
UNIT_DEST="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run this script with sudo so it can install the systemd unit."
  exit 1
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "Service user does not exist: $SERVICE_USER"
  exit 1
fi

if [[ ! -f "$UNIT_SOURCE" ]]; then
  echo "Missing unit template: $UNIT_SOURCE"
  exit 1
fi

if [[ ! -d "$INSTALL_DIR" ]]; then
  echo "Install directory does not exist: $INSTALL_DIR"
  exit 1
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python virtualenv executable not found: $PYTHON_BIN"
  exit 1
fi

if [[ ! -f "$RUNNER_PATH" ]]; then
  echo "Bot launcher not found: $RUNNER_PATH"
  exit 1
fi

if [[ ! -f "$START_SCRIPT_PATH" ]]; then
  echo "Bot start script not found: $START_SCRIPT_PATH"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Environment file not found: $ENV_FILE"
  exit 1
fi

chmod 0755 "$START_SCRIPT_PATH"

sed \
  -e "s|^User=.*$|User=$SERVICE_USER|" \
  -e "s|^Group=.*$|Group=$SERVICE_GROUP|" \
  -e "s|^WorkingDirectory=.*$|WorkingDirectory=$INSTALL_DIR|" \
  -e "s|^Environment=DC_BOT_ENV_FILE=.*$|Environment=DC_BOT_ENV_FILE=$ENV_FILE|" \
  -e "s|^EnvironmentFile=.*$|EnvironmentFile=-$ENV_FILE|" \
  -e "s|^Environment=BOT_GIT_REMOTE=.*$|Environment=BOT_GIT_REMOTE=$GIT_REMOTE|" \
  -e "s|^Environment=BOT_GIT_BRANCH=.*$|Environment=BOT_GIT_BRANCH=$GIT_BRANCH|" \
  -e "s|^Environment=BOT_AUTO_PIP_INSTALL=.*$|Environment=BOT_AUTO_PIP_INSTALL=$AUTO_PIP_INSTALL|" \
  -e "s|^Environment=PYTHON_BIN=.*$|Environment=PYTHON_BIN=$PYTHON_BIN|" \
  -e "s|^Environment=RUNNER_PATH=.*$|Environment=RUNNER_PATH=$RUNNER_PATH|" \
  -e "s|^ExecStart=.*$|ExecStart=$START_SCRIPT_PATH|" \
  "$UNIT_SOURCE" > "$UNIT_DEST"

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
systemctl --no-pager --full status "$SERVICE_NAME"
