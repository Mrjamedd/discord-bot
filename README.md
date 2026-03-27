# Discord Purchase Bot

This repository is structured to support a fresh-clone deployment on a cloud server or in Docker.

What is included:
- Bot source code
- A root launcher (`run_bot.py`) that bootstraps `.env` and optional private asset provisioning before startup
- `requirements.txt`
- Docker support
- A systemd unit template for VM deployments

What is intentionally excluded:
- Runtime state, purchase logs, and recovery files
- `.env` files and credentials

## Requirements

- Python 3.12+
- A Discord bot token
- Discord channel/category/role IDs for the purchase and support flows
- Discord Message Content intent enabled in the Discord developer portal for the bot application
- Gmail API OAuth credentials for receipt parsing
- Private `.gpc` files available to the bot, either bundled in the repo, mounted from a server directory, or provisioned from a base64-encoded zip archive secret

## Fresh-Clone Quick Start

1. Copy `.env.example` to `.env`.
2. Fill in the required Discord, Gmail, and payment values.
3. Provide the private `.gpc` files using one of these methods:
   - Preferred for Ubuntu/cloud secret stores: set `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` to a base64-encoded zip archive that contains the 4 required `.gpc` files. If `SCRIPT_FILES_DIR` is blank or left at the local repo default, the launcher will extract the files into `DC_BOT_RUNTIME_DIR/private_gpc`, which defaults to `runtime/private_gpc`.
   - Preferred for mounted storage: upload the `.gpc` files to the server and set `SCRIPT_FILES_DIR` to that directory.
4. Install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

5. Start the bot:

```bash
python run_bot.py
```

The launcher reads `.env` automatically, falls back to `$HOME/.env` when the repo file is absent, provisions `.gpc` files from `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` when present, and then starts the bot. Startup fails fast if required Discord IDs, Gmail credentials, payment settings, or delivery files are missing. Runtime bot and parser errors are streamed to stdout/stderr and, when Google Sheets is configured, written to a dedicated error-report tab in the same spreadsheet document used for purchase logging.

## Cloud Deployment Notes

- Runtime state, purchase logs, and recovery files default to `runtime/` and are ignored by Git.
- The launcher first checks for a repo-local `.env`. If that file is missing, it falls back to `$HOME/.env` when that file contains bot settings, which matches deployments that keep secrets in `/home/ubuntu/.env`.
- Override `DC_BOT_RUNTIME_DIR` if your server uses a different writable path.
- You can also override the individual runtime paths with `DC_BOT_DATA_DIR`, `DC_BOT_STATE_FILE`, `DC_BOT_PURCHASE_LOG_FILE`, and `DC_BOT_PURCHASE_SYNC_RECOVERY_FILE`. The legacy `DC_BOT_LOG_DIR`, `DC_BOT_LOG_FILE`, and `DC_BOT_PAYMENT_PARSER_LOG_FILE` settings are deprecated because runtime error reporting no longer writes rotating local log files.
- When `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` is set, the launcher strips whitespace from the base64 payload, extracts the archive into `DC_BOT_RUNTIME_DIR/private_gpc` by default, and points `SCRIPT_FILES_DIR` there automatically. Set `SCRIPT_FILES_DIR` only if you want a different extraction or mounted path.
- Google Sheets logging is optional. If Sheets credentials or the spreadsheet ID are missing, purchases are still logged locally to the runtime data directory and runtime error reports remain available in stdout/stderr only.
- Set either `GOOGLE_SHEETS_CREDENTIALS_FILE` or `GOOGLE_SHEETS_CREDENTIALS_JSON`.
- Set `GOOGLE_SHEETS_ERROR_TAB_NAME` to control the tab that stores structured runtime error reports. The bot creates that tab automatically when it is missing.
- `GOOGLE_SHEETS_CREDENTIALS_JSON` is supported for platforms that inject service-account JSON as a secret instead of mounting a file.
- The bot does not load private assets from Git. Keep `.gpc` files outside the repository, inject them with `DC_BOT_GPC_ARCHIVE_ZIP_BASE64`, or point `SCRIPT_FILES_DIR` at a mounted directory.

## OCI VM Deployment

Use `systemd` as the process manager on the OCI Ubuntu VM. That gives you automatic startup on boot, restart-on-failure, one place to inspect logs with `journalctl`, and a clean place to run the repo update step before the bot launches.

1. Clone the repo onto the server. The default unit template assumes `/opt/discord-purchase-bot`, but the install helper can render the service for any absolute path.
2. Create the bot env file from `.env.example`. On OCI Ubuntu, the default service expects `/home/ubuntu/.env`.
3. For the simplest VM deployment, leave `SCRIPT_FILES_DIR` blank and set `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` in the env file. If you prefer mounted storage instead, upload the `.gpc` files and point `SCRIPT_FILES_DIR` at that directory.
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

cd /opt/discord-purchase-bot
cp .env.example /home/ubuntu/.env

python3 -m venv venv
venv/bin/pip install -r requirements.txt

sudo ./deploy/systemd/install_oci_service.sh
```

Useful service commands on the server:

```bash
sudo systemctl status discord-purchase-bot
sudo journalctl -u discord-purchase-bot -f
sudo systemctl restart discord-purchase-bot
sudo systemctl stop discord-purchase-bot
```

How startup works with the default unit now:
- `systemd` starts `deploy/systemd/start_bot.sh`
- the wrapper verifies the repo is on `main` and clean
- it fetches `origin/main` and fast-forwards the local checkout when remote changes exist
- it runs `venv/bin/pip install -r requirements.txt` only when `requirements.txt` changed
- it launches `python run_bot.py`

If the live checkout is dirty, startup fails instead of overwriting local changes. That keeps the server copy aligned with GitHub instead of silently diverging.

If the repo lives somewhere other than `/opt/discord-purchase-bot`, render the unit with overrides:

```bash
sudo INSTALL_DIR=/srv/discord-bot \
  ENV_FILE=/home/ubuntu/.env \
  SERVICE_USER=ubuntu \
  ./deploy/systemd/install_oci_service.sh
```

If you prefer not to use `systemd`, any other process manager is acceptable as long as it starts `python run_bot.py` from the repo root with the same environment, but `systemd` is the recommended OCI VM deployment path for this project.

## Docker

Build:

```bash
docker build -t discord-purchase-bot .
```

Run:

```bash
docker run --env-file .env \
  -v /secure/gpc:/srv/gpc:ro \
  -e SCRIPT_FILES_DIR=/srv/gpc \
  discord-purchase-bot
```

If you use Google Sheets with a mounted credentials file, mount that path too. If your platform supports secret injection, prefer `GOOGLE_SHEETS_CREDENTIALS_JSON`. If you do not want to mount `.gpc` files, inject `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` instead.

## Building The Private GPC Archive

Create a zip file containing these filenames anywhere in the archive:
- `Corex-Aim_2K26.gpc`
- `GOLDEN_FREE_v2.gpc`
- `secretofscript(unrealeased) (1).gpc`
- `SWOOSH V2.gpc`

Then base64-encode the zip and store the result in `DC_BOT_GPC_ARCHIVE_ZIP_BASE64`.

Ubuntu/Linux example:

```bash
zip private_gpc.zip Corex-Aim_2K26.gpc GOLDEN_FREE_v2.gpc \
  "secretofscript(unrealeased) (1).gpc" "SWOOSH V2.gpc"
base64 -w 0 private_gpc.zip > private_gpc.zip.b64
```

The value stored in `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` must be a single-line base64 string.
