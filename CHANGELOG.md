# Changelog

## Unreleased

### Deployment

- Bundled the Gmail receipt parser into the repository so a fresh clone no longer depends on a sibling `discord_bot_private` checkout.
- Declared the clean-deployment parser dependencies explicitly with `requests` and `tzdata`.
- Replaced real admin-email defaults with blank optional settings in `.env.example` and runtime config.
- Updated the deployment docs to reflect the bundled parser flow and the actual dirty-checkout behavior of the OCI startup wrapper.

## v0.9.0 - 2026-03-27

First tagged pre-1.0 release for the current codebase.

### Highlights

- Migrated delivery assets to the canonical cloud directory at `/home/ubuntu/discord-bot/assets`.
- Removed the old environment-variable and archive/base64 asset bootstrap flow.
- Expanded Google Sheets logging into a structured purchase-flow audit trail while preserving the existing error log path.
- Added admin-only bypass testing commands with explicit audit logging markers.
- Aligned OCI/systemd deployment around `discordbot.service` with reliable auto-update behavior.
- Switched the purchase catalog to scan the live `.gpc` files in the dedicated assets folder.

### Changes Since The Previous Untagged State

- Asset loading now comes directly from the dedicated cloud asset folder instead of environment-driven extraction or bootstrap helpers.
- Startup validation now fails clearly when the asset directory is missing, unreadable, empty, or does not contain delivery `.gpc` files.
- Purchase prompts, confirmation messages, and admin catalog views now reflect the asset-backed catalog, including dynamic discovery of additional `.gpc` files.
- The known `GOLDEN_FREE_v2.gpc` asset is now displayed to customers as `Golden V2` while preserving legacy selection aliases.
- Purchase-flow auditing now records ticket lifecycle actions, selection steps, payment steps, delivery attempts, support escalation, and exception paths to Google Sheets.
- Completed purchase logging, purchase audit logging, and structured error logging now coexist in the same spreadsheet with separate tabs.
- Admin bypass commands support testing actions like status checks, manual stage changes, bypass delivery, and reset flows, with all bypass actions clearly marked in audit logs.
- Deployment scripts now target `discordbot.service`, use `venv/bin/python`, and log the checked-out commit during startup.
- The auto-update wrapper now stays compatible with local runtime assets because runtime-only folders such as `assets/` are ignored from Git cleanliness checks.
- Documentation, examples, and startup notes were refreshed to match the current asset flow, Google Sheets logging, and OCI deployment process.
