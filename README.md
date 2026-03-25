# Discord Purchase Bot

This repository is structured to support a fresh-clone deployment on a cloud server or in Docker.

What is included:
- Bot source code
- A root launcher (`run_bot.py`) that bootstraps `.env` and optional private asset provisioning before startup
- `requirements.txt`
- Docker support
- A systemd unit template for VM deployments

What is intentionally excluded:
- Runtime state, logs, and recovery files
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
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

5. Start the bot:

```bash
python run_bot.py
```

The launcher reads `.env` automatically, falls back to `$HOME/.env` when the repo file is absent, provisions `.gpc` files from `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` when present, and then starts the bot. Startup fails fast if required Discord IDs, Gmail credentials, payment settings, or delivery files are missing.

## Cloud Deployment Notes

- Runtime state and logs default to `runtime/` and are ignored by Git.
- The launcher first checks for a repo-local `.env`. If that file is missing, it falls back to `$HOME/.env` when that file contains bot settings, which matches deployments that keep secrets in `/home/ubuntu/.env`.
- Override `DC_BOT_RUNTIME_DIR` if your server uses a different writable path.
- You can also override the individual runtime paths with `DC_BOT_DATA_DIR`, `DC_BOT_LOG_DIR`, `DC_BOT_STATE_FILE`, `DC_BOT_LOG_FILE`, `DC_BOT_PAYMENT_PARSER_LOG_FILE`, `DC_BOT_PURCHASE_LOG_FILE`, and `DC_BOT_PURCHASE_SYNC_RECOVERY_FILE`.
- When `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` is set, the launcher strips whitespace from the base64 payload, extracts the archive into `DC_BOT_RUNTIME_DIR/private_gpc` by default, and points `SCRIPT_FILES_DIR` there automatically. Set `SCRIPT_FILES_DIR` only if you want a different extraction or mounted path.
- Google Sheets logging is optional. If Sheets credentials or the spreadsheet ID are missing, purchases are still logged locally to the runtime data directory.
- Set either `GOOGLE_SHEETS_CREDENTIALS_FILE` or `GOOGLE_SHEETS_CREDENTIALS_JSON`.
- `GOOGLE_SHEETS_CREDENTIALS_JSON` is supported for platforms that inject service-account JSON as a secret instead of mounting a file.
- The bot does not load private assets from Git. Keep `.gpc` files outside the repository, inject them with `DC_BOT_GPC_ARCHIVE_ZIP_BASE64`, or point `SCRIPT_FILES_DIR` at a mounted directory.

## OCI VM Deployment

1. Clone the repo to the server, for example `/opt/discord-purchase-bot`.
2. Create `/home/ubuntu/.env` from `.env.example`, or export `DC_BOT_ENV_FILE` if you store the env file elsewhere.
3. For the simplest Ubuntu deployment, leave `SCRIPT_FILES_DIR` blank and set `DC_BOT_GPC_ARCHIVE_ZIP_BASE64` in `/home/ubuntu/.env`. If you prefer mounted storage instead, upload the `.gpc` files and point `SCRIPT_FILES_DIR` at them.
4. If you use Google Sheets on Ubuntu, prefer `GOOGLE_SHEETS_CREDENTIALS_JSON` in `/home/ubuntu/.env` so you do not need to mount a credentials file.
5. Install the dependencies into a virtual environment.
6. Copy `deploy/systemd/discord-purchase-bot.service` to `/etc/systemd/system/`, then adjust `User`, `Group`, `WorkingDirectory`, and `ExecStart` to match your server.
7. Run `sudo systemctl daemon-reload`.
8. Run `sudo systemctl enable --now discord-purchase-bot`.

If you prefer not to use systemd, any process manager is fine as long as it starts `python run_bot.py` from the repo root with the same environment.

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
