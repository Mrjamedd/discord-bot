# Discord Purchase Bot

This repository is structured for Ubuntu/cloud deployment and Docker, with one fixed delivery-asset location:

- `/home/ubuntu/discord-bot/assets`

The bot no longer reads asset directories from environment variables and no longer bootstraps assets from archives or base64 secrets.

## What Is Included

- Bot source code
- A root launcher (`run_bot.py`) that loads `.env` settings before startup
- `requirements.txt`
- Docker support
- A `systemd` unit template for VM deployments

## What Is Intentionally Excluded

- Runtime state, purchase logs, and recovery files
- `.env` files and credentials
- Private `.gpc` delivery assets

## Requirements

- Python 3.12+
- A Discord bot token
- Discord channel/category/role IDs for the purchase and support flows
- Discord Message Content intent enabled in the Discord developer portal
- Gmail API OAuth credentials for receipt parsing
- The delivery assets copied to `/home/ubuntu/discord-bot/assets`

## Canonical Asset Catalog

The bot expects these files in `/home/ubuntu/discord-bot/assets`:

- `Corex-Aim_2K26.gpc`
- `GOLDEN_FREE_v2.gpc`
- `secretofscript(unrealeased) (1).gpc`
- `SWOOSH V2.gpc`

Those filenames map to the product catalog as follows:

- `CoreX Aim 2K26` -> `Corex-Aim_2K26.gpc`
- `Golden Free Aim V2` -> `GOLDEN_FREE_v2.gpc`
- `Secret of Scripts V6` -> `secretofscript(unrealeased) (1).gpc`
- `Swish V2` -> `SWOOSH V2.gpc`

Startup fails fast when `/home/ubuntu/discord-bot/assets` is missing, unreadable, empty, or missing one of the required files.

## Fresh-Clone Quick Start

1. Copy `.env.example` to `.env`.
2. Fill in the required Discord, Gmail, and payment values.
3. Create `/home/ubuntu/discord-bot/assets` and copy the four `.gpc` files there.
4. Install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

5. Start the bot:

```bash
python3 run_bot.py
```

The launcher reads a repo-local `.env` automatically and falls back to `$HOME/.env` when the repo file is absent. Runtime startup also validates the canonical asset directory before the bot connects to Discord. This quick-start flow is safe to use for basic local verification before moving on to OCI deployment.

## Cloud Deployment Notes

- Runtime state, purchase logs, and recovery files default to `runtime/` and are ignored by Git.
- The launcher first checks for a repo-local `.env`. If that file is missing, it falls back to `$HOME/.env` when that file contains bot settings, which matches deployments that keep secrets in `/home/ubuntu/.env`.
- Override `DC_BOT_RUNTIME_DIR` if your server uses a different writable path.
- You can also override the individual runtime paths with `DC_BOT_DATA_DIR`, `DC_BOT_STATE_FILE`, `DC_BOT_PURCHASE_LOG_FILE`, and `DC_BOT_PURCHASE_SYNC_RECOVERY_FILE`.
- The legacy `DC_BOT_LOG_DIR`, `DC_BOT_LOG_FILE`, and `DC_BOT_PAYMENT_PARSER_LOG_FILE` settings remain deprecated because runtime error reporting no longer writes rotating local log files.
- Google Sheets logging is optional. If Sheets credentials or the spreadsheet ID are missing, completed purchases are still logged locally to the runtime data directory, but purchase audit events and structured runtime error reports remain available in stdout/stderr only.
- Set either `GOOGLE_SHEETS_CREDENTIALS_FILE` or `GOOGLE_SHEETS_CREDENTIALS_JSON`.
- Set `GOOGLE_SHEETS_TAB_NAME` to control the tab that stores completed purchase records.
- Set `GOOGLE_SHEETS_AUDIT_TAB_NAME` to control the tab that stores the full purchase-flow audit trail.
- Set `GOOGLE_SHEETS_ERROR_TAB_NAME` to control the tab that stores structured runtime error reports. The bot creates that tab automatically when it is missing.
- `GOOGLE_SHEETS_CREDENTIALS_JSON` is supported for platforms that inject service-account JSON as a secret instead of mounting a file.
- Private delivery assets are always read from `/home/ubuntu/discord-bot/assets`.

## OCI VM Deployment

Use `systemd` as the process manager on the OCI Ubuntu VM. The active unit name is `discordbot.service`, and it gives you automatic startup on boot, restart-on-failure, one place to inspect logs with `journalctl`, and a clean place to run the repo update step before the bot launches.

1. Clone the repo onto the server. The default `discordbot.service` template assumes `/opt/discord-purchase-bot`, but the install helper can render the service for any absolute path.
2. Create the bot env file from `.env.example`. On OCI Ubuntu, the default service expects `/home/ubuntu/.env`.
3. Create `/home/ubuntu/discord-bot/assets` and upload the four required `.gpc` files there.
4. If you use Google Sheets on Ubuntu, prefer `GOOGLE_SHEETS_CREDENTIALS_JSON` in the env file so you do not need to mount a credentials file.
5. Create the virtual environment and install dependencies.
6. Make sure the service user can `git fetch`/`git pull` the repo non-interactively. The default remote in this repo uses SSH, so the user running the service needs a working SSH deploy key or equivalent GitHub SSH access.
7. Install the `systemd` unit with the helper script in `deploy/systemd/`.

Example OCI Ubuntu flow:

```bash
sudo mkdir -p /opt
cd /opt
sudo git clone <your-repo-url> discord-purchase-bot
sudo chown -R ubuntu:ubuntu /opt/discord-purchase-bot

sudo mkdir -p /home/ubuntu/discord-bot/assets
sudo chown -R ubuntu:ubuntu /home/ubuntu/discord-bot

cd /opt/discord-purchase-bot
cp .env.example /home/ubuntu/.env

python3 -m venv venv
venv/bin/pip install -r requirements.txt

sudo ./deploy/systemd/install_oci_service.sh
```

Useful service commands on the server:

```bash
sudo systemctl status discordbot
sudo journalctl -u discordbot -f
sudo systemctl restart discordbot
sudo systemctl stop discordbot
```

How startup works with the default unit:

- `discordbot.service` starts `deploy/systemd/start_bot.sh`
- the wrapper verifies that `/home/ubuntu/discord-bot/assets` exists, is readable, is not empty, and still contains the required delivery files
- it verifies the repo is on `main` and clean
- it fetches `origin/main` and fast-forwards the local checkout when remote changes exist
- it installs dependencies only when `requirements.txt` changed, using `PIP_BIN` when set or `PYTHON_BIN -m pip` otherwise
- it launches `python run_bot.py`

If the live checkout is dirty, startup fails instead of overwriting local changes. That keeps the server copy aligned with GitHub instead of silently diverging.

If the repo lives somewhere other than `/opt/discord-purchase-bot`, render the unit with overrides:

```bash
sudo INSTALL_DIR=/srv/discord-bot \
  ENV_FILE=/home/ubuntu/.env \
  SERVICE_USER=ubuntu \
  ./deploy/systemd/install_oci_service.sh
```

If you prefer not to use `systemd`, any other process manager is acceptable as long as it starts `python run_bot.py` from the repo root with the same environment.

## Docker

Build:

```bash
docker build -t discord-purchase-bot .
```

Run:

```bash
docker run --env-file .env \
  -v /secure/gpc:/home/ubuntu/discord-bot/assets:ro \
  discord-purchase-bot
```

If you use Google Sheets with a mounted credentials file, mount that path too. If your platform supports secret injection, prefer `GOOGLE_SHEETS_CREDENTIALS_JSON`.
