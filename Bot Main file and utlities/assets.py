from __future__ import annotations

import os
import re
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


KNOWN_SCRIPT_PRODUCT_CATALOG: tuple[ScriptProductCatalogEntry, ...] = (
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
        label="Golden V2",
        filename="GOLDEN_FREE_v2.gpc",
        aliases=(
            "golden",
            "golden free",
            "golden aim",
            "golden free aim",
            "golden v2",
            "golden aim v2",
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
KNOWN_SCRIPT_PRODUCTS_BY_FILENAME: dict[str, ScriptProductCatalogEntry] = {
    entry.filename: entry for entry in KNOWN_SCRIPT_PRODUCT_CATALOG
}
NUMBER_WORD_ALIASES: dict[int, str] = {
    1: "one",
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
    10: "ten",
}


def _dedupe_aliases(*alias_groups: Sequence[str]) -> tuple[str, ...]:
    aliases: list[str] = []
    seen: set[str] = set()
    for alias_group in alias_groups:
        for alias in alias_group:
            cleaned_alias = alias.strip()
            if not cleaned_alias:
                continue
            alias_key = cleaned_alias.lower()
            if alias_key in seen:
                continue
            seen.add(alias_key)
            aliases.append(cleaned_alias)
    return tuple(aliases)


def _number_aliases(index: int) -> tuple[str, ...]:
    aliases = [str(index)]
    word_alias = NUMBER_WORD_ALIASES.get(index)
    if word_alias is not None:
        aliases.append(word_alias)
    return tuple(aliases)


def _list_delivery_asset_files(asset_dir: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            (
                entry
                for entry in asset_dir.iterdir()
                if entry.is_file() and entry.suffix.lower() == ".gpc"
            ),
            key=lambda entry: entry.name.lower(),
        )
    )


def _build_fallback_script_products(*, asset_dir: Path) -> tuple[ScriptProduct, ...]:
    return tuple(
        ScriptProduct(
            key=entry.key,
            label=entry.label,
            price=SCRIPT_PRODUCT_PRICE,
            file_path=asset_dir / entry.filename,
            aliases=_dedupe_aliases(_number_aliases(index), entry.aliases),
        )
        for index, entry in enumerate(KNOWN_SCRIPT_PRODUCT_CATALOG, start=1)
    )


def _humanize_filename_stem(stem: str) -> str:
    cleaned_stem = re.sub(r"[_-]+", " ", stem)
    cleaned_stem = re.sub(r"[^A-Za-z0-9]+", " ", cleaned_stem)
    cleaned_stem = re.sub(r"\s+", " ", cleaned_stem).strip()
    if not cleaned_stem:
        return "Script"

    words: list[str] = []
    for part in cleaned_stem.split():
        if part.isupper() and len(part) <= 4:
            words.append(part)
        elif part.lower().startswith("v") and part[1:].isdigit():
            words.append(part.upper())
        else:
            words.append(part.capitalize())
    return " ".join(words)


def _slugify_product_key(value: str, *, fallback_index: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or f"script-{fallback_index}"


def _unique_product_key(candidate_key: str, used_keys: set[str]) -> str:
    unique_key = candidate_key
    suffix = 2
    while unique_key in used_keys:
        unique_key = f"{candidate_key}-{suffix}"
        suffix += 1
    used_keys.add(unique_key)
    return unique_key


def _build_dynamic_script_products(
    asset_files: Sequence[Path],
) -> tuple[ScriptProduct, ...]:
    used_keys: set[str] = set()
    products: list[ScriptProduct] = []
    for index, asset_file in enumerate(asset_files, start=1):
        known_entry = KNOWN_SCRIPT_PRODUCTS_BY_FILENAME.get(asset_file.name)
        if known_entry is not None:
            product_key = _unique_product_key(known_entry.key, used_keys)
            product_label = known_entry.label
            aliases = _dedupe_aliases(_number_aliases(index), known_entry.aliases)
        else:
            product_key = _unique_product_key(
                _slugify_product_key(asset_file.stem, fallback_index=index),
                used_keys,
            )
            product_label = _humanize_filename_stem(asset_file.stem)
            aliases = _dedupe_aliases(_number_aliases(index), (product_label,))

        products.append(
            ScriptProduct(
                key=product_key,
                label=product_label,
                price=SCRIPT_PRODUCT_PRICE,
                file_path=asset_file,
                aliases=aliases,
            )
        )
    return tuple(products)


def build_script_products(*, asset_dir: Path = CANONICAL_ASSET_DIR) -> tuple[ScriptProduct, ...]:
    try:
        if asset_dir.exists() and asset_dir.is_dir() and os.access(asset_dir, os.R_OK | os.X_OK):
            asset_files = _list_delivery_asset_files(asset_dir)
            if asset_files:
                return _build_dynamic_script_products(asset_files)
    except OSError:
        pass

    if asset_dir == CANONICAL_ASSET_DIR:
        # Keep imports, local docs, and tests usable when the live runtime
        # asset mount is not present. Startup validation still hard-fails.
        return _build_fallback_script_products(asset_dir=asset_dir)
    return ()


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

    delivery_asset_files = [
        entry for entry in asset_files if entry.suffix.lower() == ".gpc"
    ]
    if not delivery_asset_files:
        return [
            f"Required asset directory does not contain any delivery .gpc files: {asset_dir}. "
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
