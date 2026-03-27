from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from models import ScriptProduct

CANONICAL_ASSET_DIR = Path("/home/ubuntu/discord-bot/assets")
SCRIPT_PRODUCT_PRICE = 23


@dataclass(frozen=True)
class ScriptProductCatalogEntry:
    key: str
    label: str
    filename: str
    aliases: tuple[str, ...]


SCRIPT_PRODUCT_CATALOG: tuple[ScriptProductCatalogEntry, ...] = (
    ScriptProductCatalogEntry(
        key="corex-aim-2k26",
        label="CoreX Aim 2K26",
        filename="Corex-Aim_2K26.gpc",
        aliases=(
            "1",
            "one",
            "corex",
            "corex aim",
            "corex aim 2k26",
            "corex aim nba2k26",
            "corex 2k26",
            "kobe plug and play",
        ),
    ),
    ScriptProductCatalogEntry(
        key="golden-free-aim-v2",
        label="Golden Free Aim V2",
        filename="GOLDEN_FREE_v2.gpc",
        aliases=(
            "2",
            "two",
            "golden",
            "golden free",
            "golden free aim",
            "golden free v2",
            "golden free aim v2",
        ),
    ),
    ScriptProductCatalogEntry(
        key="secret-of-scripts-v6",
        label="Secret of Scripts V6",
        filename="secretofscript(unrealeased) (1).gpc",
        aliases=(
            "3",
            "three",
            "secret",
            "secret script",
            "secret of scripts",
            "secret of scripts v6",
            "secretofscript",
            "secretofscripts",
            "the god of scripts",
            "thegodofscripts",
            "secret unreleased",
        ),
    ),
    ScriptProductCatalogEntry(
        key="swish-v2",
        label="Swish V2",
        filename="SWOOSH V2.gpc",
        aliases=(
            "4",
            "four",
            "swish",
            "swish v2",
            "swoosh",
            "swoosh v2",
            "featurezens",
        ),
    ),
)


def build_script_products(*, asset_dir: Path = CANONICAL_ASSET_DIR) -> tuple[ScriptProduct, ...]:
    return tuple(
        ScriptProduct(
            key=entry.key,
            label=entry.label,
            price=SCRIPT_PRODUCT_PRICE,
            file_path=asset_dir / entry.filename,
            aliases=entry.aliases,
        )
        for entry in SCRIPT_PRODUCT_CATALOG
    )


def validate_script_asset_directory(
    products: Sequence[ScriptProduct],
    *,
    asset_dir: Path = CANONICAL_ASSET_DIR,
) -> list[str]:
    if not asset_dir.exists():
        return [
            f"Required asset directory is missing: {asset_dir}. "
            "Upload the delivery .gpc files to this exact path."
        ]
    if not asset_dir.is_dir():
        return [f"Required asset path is not a directory: {asset_dir}"]
    if not os.access(asset_dir, os.R_OK | os.X_OK):
        return [
            f"Required asset directory is unreadable: {asset_dir}. "
            "Ensure the bot service user can list and read this directory."
        ]

    try:
        asset_files = [entry for entry in asset_dir.iterdir() if entry.is_file()]
    except PermissionError:
        return [
            f"Required asset directory is unreadable: {asset_dir}. "
            "Ensure the bot service user can list and read this directory."
        ]
    except OSError as exc:
        return [f"Unable to inspect asset directory {asset_dir}: {exc}"]

    if not asset_files:
        return [
            f"Required asset directory is empty: {asset_dir}. "
            "Upload the delivery .gpc files to this exact path."
        ]

    errors: list[str] = []
    for product in products:
        if not product.file_path.exists():
            errors.append(
                f"Missing delivery file for {product.label}: {product.file_path}"
            )
            continue
        if not product.file_path.is_file():
            errors.append(
                f"Delivery asset path is not a file for {product.label}: {product.file_path}"
            )
            continue
        if not os.access(product.file_path, os.R_OK):
            errors.append(
                f"Delivery file is unreadable for {product.label}: {product.file_path}"
            )
    return errors
