from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
PRICE_QUANTUM = Decimal("0.01")


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


def normalize_ticket_price_text(value: Decimal | int | str) -> str | None:
    decimal_value: Decimal
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, int):
        decimal_value = Decimal(value)
    elif isinstance(value, str):
        stripped_value = value.strip()
        if stripped_value.startswith("$"):
            stripped_value = stripped_value[1:].strip()
        if not stripped_value:
            return None
        try:
            decimal_value = Decimal(stripped_value)
        except (InvalidOperation, ValueError):
            return None
    else:
        return None

    if not decimal_value.is_finite() or decimal_value <= 0:
        return None
    return format(decimal_value.quantize(PRICE_QUANTUM), "f")


def resolve_ticket_price_text(
    product: ScriptProduct | None,
    *,
    ticket_price_override: str | None = None,
) -> str | None:
    if ticket_price_override is not None:
        normalized_override = normalize_ticket_price_text(ticket_price_override)
        if normalized_override is not None:
            return normalized_override
    if product is None:
        return None
    return normalize_ticket_price_text(product.price)


def build_selected_product_price_text(
    product: ScriptProduct,
    *,
    ticket_price_override: str | None = None,
) -> str:
    effective_price = resolve_ticket_price_text(
        product,
        ticket_price_override=ticket_price_override,
    ) or "0.00"
    standard_price = normalize_ticket_price_text(product.price) or "0.00"
    if effective_price == standard_price:
        return f"{product.label} - ${effective_price}"
    return (
        f"{product.label} - ${effective_price} "
        f"(admin-set ticket price; standard ${standard_price})"
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
        "Reply with the exact script name, number, delivery filename, or alias."
    )


def build_ticket_panel_message() -> str:
    return (
        "Ready to buy a script?\n"
        "Press `Open Purchase Ticket` below to open a private purchase ticket and start your order.\n\n"
        "Available scripts:\n"
        f"{build_panel_catalog_lines()}"
    )


def build_support_ticket_panel_message() -> str:
    return SUPPORT_TICKET_PANEL_MESSAGE


def build_ticket_management_note() -> str:
    return (
        "Available actions:\n"
        f"- Type `{CHANGE_SCRIPT_COMMAND}` to clear your script choice and payment setup before delivery.\n"
        f"- Type `{CLOSE_TICKET_COMMAND}` to close this purchase ticket. The channel will be deleted a few seconds later.\n"
        f"- Completed purchase tickets close automatically {PURCHASE_TICKET_AUTO_CLOSE_MINUTES} minutes after delivery."
    )


def build_ticket_store_message(username: str) -> str:
    return (
        f"Welcome, {username}. This is your private purchase ticket.\n"
        f"Do this now: {build_script_selection_instruction()}\n"
        "What happens next: I will match your script and ask you to confirm it before payment.\n\n"
        "Script catalog:\n"
        f"{build_ticket_catalog_lines()}\n\n"
        f"{build_ticket_management_note()}"
    )


def build_ticket_retry_message(*, include_confirmation_hint: bool = False) -> str:
    confirmation_hint = (
        f"\nIf the script already shown is correct, type `{CONFIRM_SELECTION_RESPONSE}` exactly to continue."
        if include_confirmation_hint
        else ""
    )
    return (
        "I couldn't match that reply to a script yet.\n"
        f"Do this now: {build_script_selection_instruction()}\n"
        "What happens next: I will show the script I matched and ask you to confirm it.\n"
        "You can scroll up in this ticket to see the full catalog again."
        f"{confirmation_hint}"
    )


def build_ticket_change_script_message() -> str:
    return (
        "Your previous script and payment setup have been cleared.\n"
        f"Do this now: {build_script_selection_instruction()}\n"
        "What happens next: I will match your new script and ask you to confirm it before payment.\n\n"
        "Script catalog:\n"
        f"{build_ticket_catalog_lines()}\n\n"
        f"{build_ticket_management_note()}"
    )


def build_script_confirmation_message(
    product: ScriptProduct,
    *,
    ticket_price_override: str | None = None,
) -> str:
    return (
        "Matched script:\n"
        f"{build_selected_product_price_text(product, ticket_price_override=ticket_price_override)} "
        f"- delivery file: {product.file_path.name}\n"
        f"Do this now: type `{CONFIRM_SELECTION_RESPONSE}` exactly to confirm this script.\n"
        f"Only `{CONFIRM_SELECTION_RESPONSE}` moves you forward.\n"
        "What happens next: after you confirm, I will show the payment platform button.\n"
        "If this is the wrong script, reply with the correct script name, number, delivery filename, or alias instead."
    )


def build_payment_platform_prompt_message(
    product: ScriptProduct,
    *,
    ticket_price_override: str | None = None,
) -> str:
    available_platforms = ", ".join(
        platform.label for platform in PAYMENT_PLATFORMS
    )
    availability_text = (
        f"{PAYMENT_PLATFORMS[0].label} is currently the only available option."
        if len(PAYMENT_PLATFORMS) == 1
        else f"Available right now: {available_platforms}."
    )
    return (
        "Script confirmed:\n"
        f"{build_selected_product_price_text(product, ticket_price_override=ticket_price_override)}\n"
        "Do this now: press the button below to reveal the exact payment instructions and your note code.\n"
        f"{availability_text}\n"
        "What happens next: I will show where to send payment, how much to send, the exact note code, and the button to check your payment."
    )


def build_payment_instruction_message(
    product: ScriptProduct,
    platform: PaymentPlatform,
    payment_note_code: str,
    *,
    ticket_price_override: str | None = None,
) -> str:
    effective_price = resolve_ticket_price_text(
        product,
        ticket_price_override=ticket_price_override,
    ) or "0.00"
    standard_price = normalize_ticket_price_text(product.price) or "0.00"
    payment_amount_instruction = (
        f"Script: {product.label} at the standard ticket price of ${effective_price}."
        if effective_price == standard_price
        else (
            f"Script: {product.label} at the admin-set ticket price of ${effective_price} "
            f"(standard price ${standard_price})."
        )
    )
    return (
        f"{platform.label} payment instructions\n"
        f"{payment_amount_instruction}\n\n"
        "Do this now:\n"
        f"1. Send ${effective_price} to the {platform.destination_label} {platform.destination_value}.\n"
        f"2. Put this exact code in the payment note: `{payment_note_code}`\n"
        "3. After you pay, press `Check My Payment` below.\n\n"
        "Important:\n"
        f"- The payment note must be exactly `{payment_note_code}`.\n"
        "- If the code is missing or changed, automatic verification will fail.\n\n"
        "What happens next:\n"
        f"- After you press `Check My Payment`, I will check for your payment in about {PAYMENT_CHECK_DELAY_SECONDS} seconds.\n"
        f"- If payment is detected, I will automatically deliver `{product.file_path.name}`.\n"
        "- If automatic verification fails after a real payment, open a support ticket from the support panel for manual review."
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
