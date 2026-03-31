from __future__ import annotations

from dataclasses import dataclass
import re
import secrets

import discord

from assets import build_script_products
from config import (
    CASH_APP_CASHTAG,
    CONFIRM_SELECTION_RESPONSE,
    PAYMENT_CHECK_DELAY_SECONDS,
    SUPPORT_TICKET_PANEL_MESSAGE,
)
from models import PaymentPlatform, ScriptProduct
from utils import build_channel_name, normalize_text

SCRIPT_PRODUCTS: tuple[ScriptProduct, ...] = build_script_products()

SCRIPT_PRODUCTS_BY_KEY: dict[str, ScriptProduct] = {
    product.key: product for product in SCRIPT_PRODUCTS
}
PAYMENT_PLATFORMS = (
    PaymentPlatform(
        key="cash-app",
        label="Cash App",
        destination_label="Cash App tag",
        destination_value=CASH_APP_CASHTAG,
    ),
)
PAYMENT_PLATFORMS_BY_KEY: dict[str, PaymentPlatform] = {
    platform.key: platform for platform in PAYMENT_PLATFORMS
}
CHANGE_SCRIPT_COMMAND = "change script"
CLOSE_TICKET_COMMAND = "close ticket"
PURCHASE_TICKET_AUTO_CLOSE_MINUTES = 30
PURCHASE_TICKET_AUTO_CLOSE_DELAY_SECONDS = PURCHASE_TICKET_AUTO_CLOSE_MINUTES * 60
LEGACY_SCRIPT_PRODUCT_KEY_MAP: dict[str, str] = {
    "secret-script": "secret-of-scripts-v6",
    "infinante-stamina": "golden-free-aim-v2",
    "unlimited-vc": "swish-v2",
}
TICKET_STAGE_AWAITING_SELECTION = "awaiting_selection"
TICKET_STAGE_AWAITING_CONFIRMATION = "awaiting_confirmation"
TICKET_STAGE_AWAITING_PAYMENT_PLATFORM = "awaiting_payment_platform"
TICKET_STAGE_AWAITING_PAYMENT = "awaiting_payment"
TICKET_STAGE_PAYMENT_PENDING = "payment_pending"
TICKET_STAGE_COMPLETED = "completed"
VALID_TICKET_STAGES: set[str] = {
    TICKET_STAGE_AWAITING_SELECTION,
    TICKET_STAGE_AWAITING_CONFIRMATION,
    TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
    TICKET_STAGE_AWAITING_PAYMENT,
    TICKET_STAGE_PAYMENT_PENDING,
    TICKET_STAGE_COMPLETED,
}
UNSET = object()


@dataclass(frozen=True)
class ScriptProductSelectionResult:
    product: ScriptProduct | None
    status: str
    candidate_keys: tuple[str, ...] = ()


def build_catalog_line(
    product: ScriptProduct,
    *,
    prefix: str,
) -> str:
    return (
        f"{prefix}{product.label} - ${product.price} "
        f"(delivery file: {product.file_path.name})"
    )


def build_panel_catalog_lines() -> str:
    return "\n".join(
        build_catalog_line(product, prefix="- ")
        for product in SCRIPT_PRODUCTS
    )


def build_ticket_catalog_lines() -> str:
    return "\n".join(
        build_catalog_line(product, prefix=f"{index}. ")
        for index, product in enumerate(SCRIPT_PRODUCTS, start=1)
    )


def build_script_selection_instruction() -> str:
    return (
        "Reply with the script's exact name, number, delivery filename, or a clear alias."
    )


def build_ticket_panel_message() -> str:
    return (
        "The full asset-backed script catalog is listed below.\n"
        f"{build_panel_catalog_lines()}\n"
        "Press the button below to continue with purchase."
    )


def build_support_ticket_panel_message() -> str:
    return SUPPORT_TICKET_PANEL_MESSAGE


def build_ticket_management_note() -> str:
    return (
        f'Need a different script before delivery? Type `{CHANGE_SCRIPT_COMMAND}`.\n'
        f'If you want to start over completely, type `{CLOSE_TICKET_COMMAND}` to close this purchase ticket and then open a new one.\n'
        f"Completed purchase tickets close automatically {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes after delivery."
    )


def build_ticket_store_message(username: str) -> str:
    return (
        f"Welcome, {username}. This ticket can be used to purchase scripts.\n"
        "Available scripts, prices, and delivery files:\n"
        f"{build_ticket_catalog_lines()}\n"
        f"{build_script_selection_instruction()}\n\n"
        f"{build_ticket_management_note()}"
    )


def build_ticket_retry_message(*, include_confirmation_hint: bool = False) -> str:
    confirmation_hint = (
        f"\nType {CONFIRM_SELECTION_RESPONSE} to confirm and proceed if the current selection is already correct."
        if include_confirmation_hint
        else ""
    )
    return (
        "I couldn't tell which script you want yet.\n"
        "Available scripts, prices, and delivery files:\n"
        f"{build_ticket_catalog_lines()}\n"
        f"{build_script_selection_instruction()}{confirmation_hint}"
    )


def build_ticket_change_script_message() -> str:
    return (
        "Your current script selection has been cleared.\n"
        "Available scripts, prices, and delivery files:\n"
        f"{build_ticket_catalog_lines()}\n"
        f"{build_script_selection_instruction()}\n\n"
        f"{build_ticket_management_note()}"
    )


def build_script_confirmation_message(product: ScriptProduct) -> str:
    return (
        "Confirming this is the script you want:\n"
        f"{product.label} - ${product.price} - delivery file: {product.file_path.name}\n"
        f"Type {CONFIRM_SELECTION_RESPONSE} to confirm and proceed."
    )


def build_payment_platform_prompt_message(product: ScriptProduct) -> str:
    available_platforms = ", ".join(
        platform.label for platform in PAYMENT_PLATFORMS
    )
    availability_text = (
        f"{PAYMENT_PLATFORMS[0].label} is currently the only available option."
        if len(PAYMENT_PLATFORMS) == 1
        else f"Available right now: {available_platforms}."
    )
    return (
        f"Your script is confirmed as {product.label} - ${product.price}.\n"
        "Which payment platform would you like to use?\n"
        f"{availability_text}\n"
        "Select a payment platform below to continue."
    )


def build_payment_instruction_message(
    product: ScriptProduct,
    platform: PaymentPlatform,
    payment_note_code: str,
) -> str:
    return (
        f"{platform.label} selected as your payment method.\n"
        f"Send the amount for the script to the {platform.destination_label} "
        f"{platform.destination_value}. For this ticket, send ${product.price} for "
        f"{product.label}.\n\n"
        "Important: put this exact code in the payment note/message.\n"
        f"`{payment_note_code}`\n"
        "Use the code exactly as shown. If the receipt email does not contain this code, "
        "the product will not be sent automatically.\n\n"
        "Once the payment is sent, press Confirm Payment below. The system will check for the "
        f"payment and automatically deliver the `{product.file_path.name}` file within about {PAYMENT_CHECK_DELAY_SECONDS} "
        "seconds if the payment is detected.\n\n"
        "If you experience any issues (for example, the full amount was sent but the "
        "file was not received), please open a support ticket and a moderator will "
        "assist you shortly.\n\n"
        "Thank you!"
    )


def generate_payment_note_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "ZEN-" + "".join(secrets.choice(alphabet) for _ in range(6))


def ticket_owner_id_from_topic(topic: str | None) -> int | None:
    if not topic:
        return None
    match = re.fullmatch(r"Ticket owner:\s*(\d+)", topic.strip())
    if match is None:
        return None
    return int(match.group(1))


def message_contains_alias(normalized_message: str, alias: str) -> bool:
    normalized_alias = normalize_text(alias)
    if not normalized_alias:
        return False
    padded_message = f" {normalized_message} "
    padded_alias = f" {normalized_alias} "
    return padded_alias in padded_message


def resolve_script_product_selection(selection: str) -> ScriptProductSelectionResult:
    normalized_selection = normalize_text(selection)
    if not normalized_selection:
        return ScriptProductSelectionResult(
            product=None,
            status="empty",
        )

    best_products: list[ScriptProduct] = []
    best_score = 0
    for product in SCRIPT_PRODUCTS:
        candidate_aliases = (
            product.label,
            product.file_path.stem,
            product.file_path.name,
            *product.aliases,
        )
        product_score = max(
            (
                len(normalize_text(alias))
                for alias in candidate_aliases
                if message_contains_alias(normalized_selection, alias)
            ),
            default=0,
        )
        if product_score == 0:
            continue
        if product_score > best_score:
            best_products = [product]
            best_score = product_score
        elif product_score == best_score:
            best_products.append(product)

    if best_score == 0:
        return ScriptProductSelectionResult(
            product=None,
            status="unmatched",
        )
    if len(best_products) > 1:
        return ScriptProductSelectionResult(
            product=None,
            status="ambiguous",
            candidate_keys=tuple(product.key for product in best_products),
        )
    return ScriptProductSelectionResult(
        product=best_products[0],
        status="matched",
        candidate_keys=(best_products[0].key,),
    )


def find_script_product(selection: str) -> ScriptProduct | None:
    return resolve_script_product_selection(selection).product


def resolve_script_product_key(product_key: str | None) -> str | None:
    if product_key is None:
        return None
    return LEGACY_SCRIPT_PRODUCT_KEY_MAP.get(product_key, product_key)


def get_script_product_by_key(product_key: str | None) -> ScriptProduct | None:
    resolved_product_key = resolve_script_product_key(product_key)
    if resolved_product_key is None:
        return None
    return SCRIPT_PRODUCTS_BY_KEY.get(resolved_product_key)


def get_payment_platform_by_key(platform_key: str | None) -> PaymentPlatform | None:
    if platform_key is None:
        return None
    return PAYMENT_PLATFORMS_BY_KEY.get(platform_key)



def message_is_selection_confirmation(message_content: str) -> bool:
    return normalize_text(message_content) == CONFIRM_SELECTION_RESPONSE


def message_requests_script_change(message_content: str) -> bool:
    return normalize_text(message_content) == CHANGE_SCRIPT_COMMAND


def message_requests_ticket_close(message_content: str) -> bool:
    return normalize_text(message_content) == CLOSE_TICKET_COMMAND


def build_script_delivery_file(product: ScriptProduct) -> discord.File:
    if not product.file_path.is_file():
        raise FileNotFoundError(product.file_path)
    return discord.File(
        fp=product.file_path,
        filename=product.file_path.name,
    )


def build_ticket_channel_name(username: str) -> str:
    return build_channel_name(username, prefix="ticket")


def build_support_ticket_channel_name(username: str) -> str:
    return build_channel_name(username, prefix="support-ticket")


def ticket_owner_topic(user_id: int) -> str:
    return f"Ticket owner: {user_id}"
