from __future__ import annotations

import base64
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.header import decode_header
from email.utils import getaddresses, parseaddr
from html import unescape
from typing import Any, cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    ALLOWED_CURRENCY,
    ALLOWED_FROM_ADDRESSES,
    ALLOWED_FROM_DOMAINS,
    ALLOWED_FROM_SUBDOMAINS,
    GMAIL_API_CLIENT_ID_ENV,
    GMAIL_API_CLIENT_SECRET_ENV,
    GMAIL_API_REFRESH_TOKEN_ENV,
    GMAIL_API_SCOPES,
    GMAIL_API_TOKEN_URI,
    GMAIL_API_USER_ID,
    MAX_MESSAGE_AGE_HOURS,
    MAX_MESSAGES_TO_SCAN,
    NEGATIVE_TIME_BUFFER_MINUTES,
    PAYMENT_PARSER_GMAIL_ADDRESS,
    PAYMENT_PARSER_EXPECTED_AMOUNT,
    POSITIVE_TIME_WINDOW_MINUTES,
    REQUIRE_DMARC_WHEN_AVAILABLE,
    REQUIRE_STRICT_ALIGNMENT,
    REQUIRE_STRICT_FROM_ADDRESS_ALLOWLIST,
    REJECT_FORWARDED,
    REJECT_PASTED_STRONG_ONLY,
    REJECT_RESENT,
)
from models import PaymentParserResult


LOGGER = logging.getLogger("dc_bot.payment_parser")
DECIMAL_CENTS = Decimal("0.01")
TRUSTED_AUTH_RESULTS_MARKER = "mx.google.com"
FORWARD_SUBJECT_PATTERN = re.compile(r"^\s*(?:fwd|fw)\s*:", flags=re.IGNORECASE)
FORWARDED_MARKER_PATTERNS = (
    re.compile(r"^\s*-{2,}\s*forwarded message\s*-{2,}\s*$", flags=re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*begin forwarded message\s*:\s*$", flags=re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*-{2,}\s*original message\s*-{2,}\s*$", flags=re.IGNORECASE | re.MULTILINE),
)
WEAK_FORWARD_PATTERNS = (
    re.compile(r"\bforwarded\b", flags=re.IGNORECASE),
    re.compile(r"\boriginal message\b", flags=re.IGNORECASE),
)
PAYMENT_KEYWORD_PATTERNS = (
    re.compile(r"\bpayment received\b", flags=re.IGNORECASE),
    re.compile(r"\bsent you\b", flags=re.IGNORECASE),
    re.compile(r"\byou received\b", flags=re.IGNORECASE),
    re.compile(r"\bpayment\b", flags=re.IGNORECASE),
    re.compile(r"\bamount\b", flags=re.IGNORECASE),
    re.compile(r"\bpaid\b", flags=re.IGNORECASE),
)
CURRENCY_HINT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(?:US\$|USD|\$)",
    flags=re.IGNORECASE,
)
AMOUNT_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])(?P<currency>US\$|USD|\$)?\s*(?P<amount>\d+(?:\.\d{1,2})?)(?![\d/])",
    flags=re.IGNORECASE,
)
HEADER_BLOCK_LINE_PATTERN = re.compile(
    r"^\s*(from|sent|to|subject|date|cc)\s*:",
    flags=re.IGNORECASE,
)
RECIPIENT_HEADER_NAMES: tuple[str, ...] = (
    "delivered-to",
    "x-original-to",
    "envelope-to",
    "to",
    "cc",
)


@dataclass(frozen=True)
class ParsedGmailMessage:
    gmail_message_id: str
    headers: dict[str, list[str]]
    subject: str
    from_header: str
    plain_text: str
    html_text: str
    received_at_utc: datetime | None


@dataclass(frozen=True)
class AmountCandidate:
    normalized_amount: Decimal
    amount_text: str
    currency: str | None
    context: str
    source: str


@dataclass(frozen=True)
class AuthEvaluation:
    passed: bool
    summary: str
    strength: int
    missing: bool


@dataclass(frozen=True)
class CandidateEvaluation:
    result: PaymentParserResult
    validation_score: int
    received_at_utc: datetime | None
    auth_strength: int


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""

    decoded_parts: list[str] = []
    for part, encoding in decode_header(value):
        if isinstance(part, bytes):
            charset = encoding or "utf-8"
            try:
                decoded_parts.append(part.decode(charset, errors="replace"))
            except LookupError:
                decoded_parts.append(part.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return "".join(decoded_parts).strip()


def _strip_html_tags(value: str) -> str:
    with_line_breaks = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", value)
    with_line_breaks = re.sub(
        r"(?i)</\s*(p|div|li|tr|table|section|article|h[1-6])\s*>",
        "\n",
        with_line_breaks,
    )
    stripped = re.sub(r"<[^>]+>", " ", with_line_breaks)
    stripped = unescape(stripped)
    stripped = stripped.replace("\r\n", "\n").replace("\r", "\n")
    stripped = re.sub(r"[ \t\f\v]+", " ", stripped)
    stripped = re.sub(r"\n{3,}", "\n\n", stripped)
    return stripped.strip()


def _normalize_fragment(value: str) -> str:
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"[ \t\f\v]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def _decode_base64url(value: str | None) -> str:
    if not value:
        return ""
    padded_value = value + ("=" * (-len(value) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded_value.encode("ascii"))
    except (ValueError, UnicodeEncodeError):
        return ""
    return decoded.decode("utf-8", errors="replace")


def _payload_is_attachment(payload: dict[str, object]) -> bool:
    filename = payload.get("filename")
    if isinstance(filename, str) and filename:
        return True

    headers = cast(list[dict[str, object]], payload.get("headers", []))
    for header in headers:
        name = header.get("name")
        value = header.get("value")
        if (
            isinstance(name, str)
            and name.lower() == "content-disposition"
            and isinstance(value, str)
            and "attachment" in value.lower()
        ):
            return True
    return False


def _collect_body_parts(
    payload: dict[str, object],
    plain_parts: list[str],
    html_parts: list[str],
) -> None:
    if _payload_is_attachment(payload):
        return

    mime_type = cast(str, payload.get("mimeType", ""))
    body = cast(dict[str, object], payload.get("body", {}))
    body_text = _decode_base64url(cast(str | None, body.get("data")))

    if mime_type == "text/plain" and body_text:
        plain_parts.append(body_text)
    elif mime_type == "text/html" and body_text:
        html_parts.append(_strip_html_tags(body_text))

    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            if isinstance(part, dict):
                _collect_body_parts(part, plain_parts, html_parts)


def _headers_from_payload(payload: dict[str, object]) -> dict[str, list[str]]:
    headers_by_name: dict[str, list[str]] = {}
    raw_headers = payload.get("headers")
    if not isinstance(raw_headers, list):
        return headers_by_name

    for raw_header in raw_headers:
        if not isinstance(raw_header, dict):
            continue
        name = raw_header.get("name")
        value = raw_header.get("value")
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        normalized_name = name.lower()
        headers_by_name.setdefault(normalized_name, []).append(
            _decode_header_value(value)
        )
    return headers_by_name


def _first_header(headers: dict[str, list[str]], name: str) -> str:
    values = headers.get(name.lower(), [])
    return values[0] if values else ""


def _parse_received_at_utc(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        timestamp_ms = int(str(value))
    except ValueError:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _parse_gmail_message(gmail_message: dict[str, object]) -> ParsedGmailMessage | None:
    message_id = gmail_message.get("id")
    if not isinstance(message_id, str) or not message_id:
        return None

    payload = gmail_message.get("payload")
    if not isinstance(payload, dict):
        return None

    headers = _headers_from_payload(payload)
    plain_parts: list[str] = []
    html_parts: list[str] = []
    _collect_body_parts(payload, plain_parts, html_parts)

    return ParsedGmailMessage(
        gmail_message_id=message_id,
        headers=headers,
        subject=_first_header(headers, "subject"),
        from_header=_first_header(headers, "from"),
        plain_text=_normalize_fragment("\n".join(part for part in plain_parts if part)),
        html_text=_normalize_fragment("\n".join(part for part in html_parts if part)),
        received_at_utc=_parse_received_at_utc(gmail_message.get("internalDate")),
    )


def _build_default_result(reason: str) -> PaymentParserResult:
    return {
        "matched": False,
        "reason": reason,
        "gmail_message_id": None,
        "from_address": None,
        "from_domain": None,
        "allowed_sender_domains": list(ALLOWED_FROM_DOMAINS),
        "allowed_sender_subdomains": list(ALLOWED_FROM_SUBDOMAINS),
        "amount": None,
        "expected_amount": None,
        "amount_shortfall": None,
        "expected_payment_note": None,
        "currency": None,
        "received_timestamp_utc": None,
        "auth_summary": "missing",
        "forwarding_flags": [],
        "amount_candidates": [],
        "weak_forwarding_flags": [],
        "timestamp_in_window": False,
        "auth_strength": 0,
        "sender_address_allowlisted": False,
        "expected_payment_note_found": False,
    }


def _normalize_decimal(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_CENTS)


def _format_decimal(value: Decimal) -> str:
    return format(_normalize_decimal(value), "f")


def _normalize_domain(value: str) -> str:
    return value.strip().lower().strip(".")


def _matching_allowed_root(from_domain: str) -> str | None:
    normalized_domain = _normalize_domain(from_domain)
    for allowed_domain in ALLOWED_FROM_DOMAINS:
        if normalized_domain == allowed_domain:
            return allowed_domain
        if normalized_domain.endswith(f".{allowed_domain}"):
            return allowed_domain
    return None


def _sender_domain_allowed(from_domain: str) -> tuple[bool, str]:
    normalized_domain = _normalize_domain(from_domain)
    if normalized_domain in ALLOWED_FROM_DOMAINS:
        return True, ""
    if normalized_domain in ALLOWED_FROM_SUBDOMAINS:
        return True, ""
    if _matching_allowed_root(normalized_domain) is not None:
        return False, "sender subdomain not explicitly approved"
    return False, "sender domain not allowed"


def _normalize_auth_domain(raw_value: str) -> str | None:
    normalized = raw_value.strip().lower().strip("><")
    if "@" in normalized:
        normalized = normalized.rsplit("@", 1)[1]
    normalized = normalized.strip(".")
    return normalized or None


def _extract_domain_from_segment(segment: str, key: str) -> str | None:
    match = re.search(
        rf"\b{re.escape(key)}=([^\s;]+)",
        segment,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    return _normalize_auth_domain(match.group(1))


def _domain_is_aligned(auth_domain: str, from_domain: str) -> bool:
    normalized_auth_domain = _normalize_domain(auth_domain)
    normalized_from_domain = _normalize_domain(from_domain)
    if not REQUIRE_STRICT_ALIGNMENT:
        return True
    return normalized_auth_domain == normalized_from_domain


def _evaluate_auth_results(
    headers: dict[str, list[str]],
    from_domain: str,
) -> AuthEvaluation:
    auth_results_headers = headers.get("authentication-results", [])
    trusted_headers: list[str] = []
    if len(auth_results_headers) == 1:
        candidate_header = auth_results_headers[0].lstrip()
        if candidate_header.lower().startswith(f"{TRUSTED_AUTH_RESULTS_MARKER};"):
            trusted_headers.append(candidate_header)
    if not trusted_headers:
        return AuthEvaluation(
            passed=True,
            summary="missing",
            strength=0,
            missing=True,
        )

    dmarc_result: str | None = None
    dmarc_domain: str | None = None
    dkim_pass_domains: list[str] = []
    spf_pass_domains: list[str] = []

    for header_value in trusted_headers:
        segments = [segment.strip() for segment in header_value.split(";") if segment.strip()]
        if dmarc_domain is None:
            header_from_match = re.search(
                r"\bheader\.from=([^\s;]+)",
                header_value,
                flags=re.IGNORECASE,
            )
            if header_from_match is not None:
                dmarc_domain = _normalize_auth_domain(header_from_match.group(1))

        for segment in segments:
            lower_segment = segment.lower()
            if lower_segment.startswith("dmarc="):
                match = re.search(r"\bdmarc=([a-z]+)", lower_segment)
                if match is not None and dmarc_result is None:
                    dmarc_result = match.group(1)
            elif lower_segment.startswith("dkim=") and "dkim=pass" in lower_segment:
                dkim_domain = _extract_domain_from_segment(segment, "header.i")
                if dkim_domain is None:
                    dkim_domain = _extract_domain_from_segment(segment, "header.d")
                if dkim_domain is not None:
                    dkim_pass_domains.append(dkim_domain)
            elif lower_segment.startswith("spf=") and "spf=pass" in lower_segment:
                spf_domain = _extract_domain_from_segment(segment, "smtp.mailfrom")
                if spf_domain is None:
                    spf_domain = _extract_domain_from_segment(segment, "smtp.helo")
                if spf_domain is not None:
                    spf_pass_domains.append(spf_domain)

    aligned_dkim_domains = [
        domain for domain in dkim_pass_domains if _domain_is_aligned(domain, from_domain)
    ]
    aligned_spf_domains = [
        domain for domain in spf_pass_domains if _domain_is_aligned(domain, from_domain)
    ]

    if dmarc_result is not None:
        dmarc_aligned = dmarc_result == "pass" and (
            dmarc_domain is None or _domain_is_aligned(dmarc_domain, from_domain)
        )
        dmarc_passed = dmarc_aligned if REQUIRE_DMARC_WHEN_AVAILABLE else True
        passed = dmarc_passed
        strength = 3 if dmarc_aligned else 0
    else:
        passed = bool(aligned_dkim_domains or aligned_spf_domains)
        if aligned_dkim_domains and aligned_spf_domains:
            strength = 2
        elif aligned_dkim_domains or aligned_spf_domains:
            strength = 1
        else:
            strength = 0

    summary_parts = [
        f"dmarc={dmarc_result or 'missing'}",
        (
            "dkim="
            + (
                ",".join(_unique_preserving_order(aligned_dkim_domains))
                if aligned_dkim_domains
                else "missing"
            )
        ),
        (
            "spf="
            + (
                ",".join(_unique_preserving_order(aligned_spf_domains))
                if aligned_spf_domains
                else "missing"
            )
        ),
    ]
    return AuthEvaluation(
        passed=passed,
        summary="; ".join(summary_parts),
        strength=strength,
        missing=False,
    )


def _extract_sender(from_header: str) -> tuple[str | None, str | None]:
    normalized_header = _decode_header_value(from_header)
    sender_address = parseaddr(normalized_header)[1].strip().lower()
    if not sender_address or "@" not in sender_address:
        return None, None
    local_part, domain = sender_address.rsplit("@", 1)
    if not local_part or not domain:
        return None, None
    return sender_address, _normalize_domain(domain)


def _normalize_email_address(value: str) -> str | None:
    normalized = value.strip().lower()
    if not normalized or "@" not in normalized:
        return None
    local_part, domain = normalized.rsplit("@", 1)
    if not local_part or not domain:
        return None
    return f"{local_part}@{_normalize_domain(domain)}"


def _extract_recipient_addresses(headers: dict[str, list[str]]) -> list[str]:
    raw_values: list[str] = []
    for header_name in RECIPIENT_HEADER_NAMES:
        raw_values.extend(headers.get(header_name, []))

    recipient_addresses: list[str] = []
    for _, raw_address in getaddresses(raw_values):
        normalized_address = _normalize_email_address(raw_address)
        if normalized_address is not None:
            recipient_addresses.append(normalized_address)
    return _unique_preserving_order(recipient_addresses)


def _message_targets_expected_recipient(
    headers: dict[str, list[str]],
) -> tuple[bool, str]:
    expected_address = _normalize_email_address(PAYMENT_PARSER_GMAIL_ADDRESS)
    if expected_address is None:
        return False, "configured recipient address invalid"

    recipient_addresses = _extract_recipient_addresses(headers)
    if not recipient_addresses:
        # Some automated receipts are delivered without a visible To/Cc style header.
        # When that happens we rely on the authenticated inbox scan plus the other parser checks.
        return True, ""
    if expected_address not in recipient_addresses:
        return False, "expected recipient not found"
    return True, ""


def _message_contains_expected_payment_note(
    headers: dict[str, list[str]],
    subject: str,
    plain_text: str,
    html_text: str,
    expected_payment_note: str,
) -> bool:
    normalized_expected_note = expected_payment_note.strip().upper()
    if not normalized_expected_note:
        return False

    searchable_parts = [subject, plain_text, html_text]
    searchable_parts.extend(
        header_value
        for values in headers.values()
        for header_value in values
    )
    searchable_text = "\n".join(part for part in searchable_parts if part).upper()
    return normalized_expected_note in searchable_text


def _split_fragments(value: str) -> list[str]:
    if not value:
        return []

    fragments: list[str] = []
    for line in value.splitlines():
        stripped_line = _normalize_fragment(line)
        if not stripped_line:
            continue
        pieces = re.split(r"(?<=[.!?])\s+|\s{2,}", stripped_line)
        for piece in pieces:
            normalized_piece = _normalize_fragment(piece)
            if normalized_piece:
                fragments.append(normalized_piece)
    return fragments


def _fragment_has_payment_keyword(fragment: str) -> bool:
    if any(pattern.search(fragment) for pattern in PAYMENT_KEYWORD_PATTERNS):
        return True

    lowered_fragment = fragment.lower()
    if CURRENCY_HINT_PATTERN.search(fragment) is None:
        return False
    return " sent " in f" {lowered_fragment} " or " received " in f" {lowered_fragment} "


def _candidate_currency(currency_token: str | None) -> str | None:
    if currency_token is None:
        return None
    if currency_token.strip().upper() in {"USD", "US$"}:
        return ALLOWED_CURRENCY
    if currency_token.strip() == "$":
        return ALLOWED_CURRENCY
    return None


def _parse_amount_candidate(
    source: str,
    fragment: str,
    match: re.Match[str],
) -> AmountCandidate | None:
    amount_text = match.group("amount")
    try:
        normalized_amount = _normalize_decimal(Decimal(amount_text))
    except InvalidOperation:
        return None

    currency = _candidate_currency(match.group("currency"))
    return AmountCandidate(
        normalized_amount=normalized_amount,
        amount_text=amount_text,
        currency=currency,
        context=fragment,
        source=source,
    )


def _extract_amount_candidates(subject: str, plain_text: str, html_text: str) -> list[AmountCandidate]:
    candidates: list[AmountCandidate] = []
    seen_candidate_keys: set[tuple[str, str, str | None]] = set()

    sources = (
        ("subject", subject),
        ("plain", plain_text),
        ("html", html_text),
    )
    for source_name, source_text in sources:
        for fragment in _split_fragments(source_text):
            if not _fragment_has_payment_keyword(fragment):
                continue
            for match in AMOUNT_TOKEN_PATTERN.finditer(fragment):
                candidate = _parse_amount_candidate(source_name, fragment, match)
                if candidate is None:
                    continue
                candidate_key = (
                    _format_decimal(candidate.normalized_amount),
                    candidate.currency or "",
                )
                if candidate_key in seen_candidate_keys:
                    continue
                seen_candidate_keys.add(candidate_key)
                candidates.append(candidate)
    return candidates


def _has_strong_forwarded_header_block(body_text: str) -> bool:
    recent_lines: list[bool] = []
    for raw_line in body_text.splitlines():
        if not raw_line.strip():
            continue
        recent_lines.append(bool(HEADER_BLOCK_LINE_PATTERN.match(raw_line)))
        if len(recent_lines) > 6:
            recent_lines.pop(0)
        if sum(recent_lines) >= 3:
            return True
    return False


def _detect_forwarding_flags(
    subject: str,
    headers: dict[str, list[str]],
    plain_text: str,
    html_text: str,
) -> tuple[list[str], list[str]]:
    strong_flags: list[str] = []
    weak_flags: list[str] = []
    combined_body = "\n".join(part for part in (plain_text, html_text) if part)

    if FORWARD_SUBJECT_PATTERN.match(subject):
        strong_flags.append("subject_forward_prefix")

    if REJECT_RESENT and any(header_name.startswith("resent-") for header_name in headers):
        strong_flags.append("resent_headers")

    for pattern in FORWARDED_MARKER_PATTERNS:
        if pattern.search(combined_body):
            strong_flags.append("forwarded_marker")
            break

    if _has_strong_forwarded_header_block(combined_body):
        strong_flags.append("quoted_header_block")
    else:
        weak_header_lines = sum(
            1 for line in combined_body.splitlines() if HEADER_BLOCK_LINE_PATTERN.match(line)
        )
        if weak_header_lines > 0:
            weak_flags.append("quoted_header_fragment")

    for pattern in WEAK_FORWARD_PATTERNS:
        if pattern.search(combined_body):
            weak_flags.append("forwarded_phrase")
            break

    return _unique_preserving_order(strong_flags), _unique_preserving_order(weak_flags)


def _within_active_window(
    received_at_utc: datetime | None,
    confirm_pressed_at_utc: datetime,
) -> bool:
    if received_at_utc is None:
        return False

    normalized_confirm_time = confirm_pressed_at_utc.astimezone(timezone.utc)
    window_start = normalized_confirm_time - timedelta(
        minutes=NEGATIVE_TIME_BUFFER_MINUTES
    )
    window_end = normalized_confirm_time + timedelta(
        minutes=POSITIVE_TIME_WINDOW_MINUTES
    )
    return window_start <= received_at_utc <= window_end


def _within_search_age(received_at_utc: datetime | None, now_utc: datetime) -> bool:
    if received_at_utc is None:
        return False
    return received_at_utc >= now_utc - timedelta(hours=MAX_MESSAGE_AGE_HOURS)


def _evaluate_candidate(
    parsed_message: ParsedGmailMessage,
    *,
    confirm_pressed_at_utc: datetime,
    expected_amount: Decimal,
    expected_payment_note: str = "",
    consumed_message_ids: set[str],
    now_utc: datetime,
) -> CandidateEvaluation:
    result = _build_default_result("no candidate messages found")
    result["gmail_message_id"] = parsed_message.gmail_message_id
    normalized_expected_amount = _normalize_decimal(expected_amount)
    normalized_expected_payment_note = expected_payment_note.strip().upper()
    result["expected_amount"] = _format_decimal(normalized_expected_amount)
    if normalized_expected_payment_note:
        result["expected_payment_note"] = normalized_expected_payment_note
        result["expected_payment_note_found"] = _message_contains_expected_payment_note(
            parsed_message.headers,
            parsed_message.subject,
            parsed_message.plain_text,
            parsed_message.html_text,
            normalized_expected_payment_note,
        )

    if parsed_message.received_at_utc is not None:
        result["received_timestamp_utc"] = parsed_message.received_at_utc.isoformat()

    sender_address, sender_domain = _extract_sender(parsed_message.from_header)
    result["from_address"] = sender_address
    result["from_domain"] = sender_domain

    validation_score = 0
    if sender_address is None or sender_domain is None:
        result["reason"] = "malformed or missing sender header"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    sender_allowed, sender_rejection_reason = _sender_domain_allowed(sender_domain)
    sender_address_allowlisted = sender_address in ALLOWED_FROM_ADDRESSES
    result["sender_address_allowlisted"] = sender_address_allowlisted
    if not sender_allowed:
        result["reason"] = sender_rejection_reason
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    recipient_matches, recipient_rejection_reason = _message_targets_expected_recipient(
        parsed_message.headers
    )
    if not recipient_matches:
        result["reason"] = recipient_rejection_reason
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    if (
        REQUIRE_STRICT_FROM_ADDRESS_ALLOWLIST
        and ALLOWED_FROM_ADDRESSES
        and not sender_address_allowlisted
    ):
        result["reason"] = "sender address suspicious"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    if parsed_message.gmail_message_id in consumed_message_ids:
        result["reason"] = "message already consumed"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    strong_forwarding_flags, weak_forwarding_flags = _detect_forwarding_flags(
        parsed_message.subject,
        parsed_message.headers,
        parsed_message.plain_text,
        parsed_message.html_text,
    )
    result["forwarding_flags"] = strong_forwarding_flags
    result["weak_forwarding_flags"] = weak_forwarding_flags
    if strong_forwarding_flags and (
        REJECT_FORWARDED or REJECT_RESENT or REJECT_PASTED_STRONG_ONLY
    ):
        result["reason"] = "forwarded or resent"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    timestamp_in_window = _within_active_window(
        parsed_message.received_at_utc,
        confirm_pressed_at_utc,
    ) and _within_search_age(parsed_message.received_at_utc, now_utc)
    result["timestamp_in_window"] = timestamp_in_window
    if not timestamp_in_window:
        result["reason"] = "outside allowed time window"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    if normalized_expected_payment_note:
        if not result["expected_payment_note_found"]:
            result["reason"] = "payment note missing"
            return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

        validation_score += 1
    amount_candidates = _extract_amount_candidates(
        parsed_message.subject,
        parsed_message.plain_text,
        parsed_message.html_text,
    )
    normalized_amount_candidates = _unique_preserving_order(
        [_format_decimal(candidate.normalized_amount) for candidate in amount_candidates]
    )
    result["amount_candidates"] = normalized_amount_candidates

    if not amount_candidates:
        result["reason"] = "amount not found"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    if len(normalized_amount_candidates) > 1:
        result["reason"] = "ambiguous amount extraction"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    selected_amount = amount_candidates[0].normalized_amount
    if not any(candidate.currency == ALLOWED_CURRENCY for candidate in amount_candidates):
        result["amount"] = _format_decimal(selected_amount)
        result["reason"] = "currency not confirmed"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    result["amount"] = _format_decimal(selected_amount)
    result["currency"] = ALLOWED_CURRENCY
    if selected_amount != normalized_expected_amount:
        shortfall = normalized_expected_amount - selected_amount
        if shortfall > Decimal("0.00"):
            result["amount_shortfall"] = _format_decimal(shortfall)
            result["reason"] = "amount short"
            return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)
        result["reason"] = "amount mismatch"
        return CandidateEvaluation(result, validation_score, parsed_message.received_at_utc, 0)

    validation_score += 1
    auth_evaluation = _evaluate_auth_results(parsed_message.headers, sender_domain)
    result["auth_summary"] = auth_evaluation.summary
    result["auth_strength"] = auth_evaluation.strength
    if not auth_evaluation.passed:
        result["reason"] = "authentication failure"
        return CandidateEvaluation(
            result,
            validation_score,
            parsed_message.received_at_utc,
            auth_evaluation.strength,
        )

    validation_score += 1
    result["matched"] = True
    result["reason"] = "matched"
    return CandidateEvaluation(
        result,
        validation_score,
        parsed_message.received_at_utc,
        auth_evaluation.strength,
    )


def _unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


def _log_candidate_decision(evaluation: CandidateEvaluation) -> None:
    result = evaluation.result
    LOGGER.info(
        "payment_parser_candidate_decision gmail_message_id=%s sender_address=%r sender_domain=%r amount_candidates=%s selected_amount=%r currency=%r forwarding_flags=%s weak_forwarding_flags=%s timestamp_in_window=%s auth_summary=%r final_decision=%s rejection_reason=%r",
        result.get("gmail_message_id"),
        result.get("from_address"),
        result.get("from_domain"),
        result.get("amount_candidates", []),
        result.get("amount"),
        result.get("currency"),
        result.get("forwarding_flags", []),
        result.get("weak_forwarding_flags", []),
        result.get("timestamp_in_window"),
        result.get("auth_summary"),
        result.get("matched"),
        None if result.get("matched") else result.get("reason"),
    )


def _select_best_rejection(
    evaluations: list[CandidateEvaluation],
    *,
    expected_payment_note: str = "",
) -> PaymentParserResult:
    if not evaluations:
        default_result = _build_default_result("no candidate messages found")
        if expected_payment_note:
            default_result["expected_payment_note"] = expected_payment_note
        return default_result

    relevant_evaluations = evaluations
    if expected_payment_note:
        relevant_evaluations = [
            evaluation
            for evaluation in evaluations
            if evaluation.result.get("expected_payment_note_found") is True
        ]
        if not relevant_evaluations:
            default_result = _build_default_result("no candidate messages found")
            default_result["expected_payment_note"] = expected_payment_note
            return default_result

    best_evaluation = max(
        relevant_evaluations,
        key=lambda evaluation: (
            evaluation.validation_score,
            evaluation.auth_strength,
            evaluation.received_at_utc or datetime.min.replace(tzinfo=timezone.utc),
        ),
    )
    return best_evaluation.result


def _select_best_valid_candidate(
    evaluations: list[CandidateEvaluation],
) -> PaymentParserResult | None:
    valid_evaluations = [evaluation for evaluation in evaluations if evaluation.result["matched"]]
    if not valid_evaluations:
        return None

    valid_evaluations.sort(
        key=lambda evaluation: (
            evaluation.received_at_utc or datetime.min.replace(tzinfo=timezone.utc),
            evaluation.auth_strength,
        ),
        reverse=True,
    )
    best_evaluation = valid_evaluations[0]
    if len(valid_evaluations) == 1:
        return best_evaluation.result

    next_evaluation = valid_evaluations[1]
    if (
        best_evaluation.received_at_utc == next_evaluation.received_at_utc
        and best_evaluation.auth_strength == next_evaluation.auth_strength
    ):
        return _build_default_result("ambiguous valid candidates")
    return best_evaluation.result


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing Gmail API credential: {name}")
    return value


def _build_gmail_service() -> Any:
    credentials = Credentials(
        token=None,
        refresh_token=_get_required_env(GMAIL_API_REFRESH_TOKEN_ENV),
        token_uri=GMAIL_API_TOKEN_URI,
        client_id=_get_required_env(GMAIL_API_CLIENT_ID_ENV),
        client_secret=_get_required_env(GMAIL_API_CLIENT_SECRET_ENV),
        scopes=list(GMAIL_API_SCOPES),
    )
    credentials.refresh(Request())
    return build(
        "gmail",
        "v1",
        credentials=credentials,
        cache_discovery=False,
    )


def _list_recent_inbox_message_ids(
    gmail_service: Any,
    *,
    unread_only: bool = True,
) -> list[str]:
    label_ids = ["INBOX"]
    if unread_only:
        label_ids.append("UNREAD")

    response = (
        gmail_service.users()
        .messages()
        .list(
            userId=GMAIL_API_USER_ID,
            labelIds=label_ids,
            maxResults=MAX_MESSAGES_TO_SCAN,
            q="newer_than:1d",
        )
        .execute()
    )
    raw_messages = response.get("messages", [])
    if not isinstance(raw_messages, list):
        return []

    message_ids: list[str] = []
    for raw_message in raw_messages:
        if not isinstance(raw_message, dict):
            continue
        message_id = raw_message.get("id")
        if isinstance(message_id, str) and message_id:
            message_ids.append(message_id)
    return message_ids


def _fetch_recent_candidates(
    gmail_service: Any,
    *,
    unread_only: bool = True,
) -> list[ParsedGmailMessage]:
    parsed_messages: list[ParsedGmailMessage] = []
    for message_id in _list_recent_inbox_message_ids(
        gmail_service,
        unread_only=unread_only,
    ):
        gmail_message = (
            gmail_service.users()
            .messages()
            .get(
                userId=GMAIL_API_USER_ID,
                id=message_id,
                format="full",
            )
            .execute()
        )
        if not isinstance(gmail_message, dict):
            continue
        parsed_message = _parse_gmail_message(gmail_message)
        if parsed_message is not None:
            parsed_messages.append(parsed_message)
    return parsed_messages


def check_payment_email(
    *,
    confirm_pressed_at_utc: datetime,
    expected_amount: Decimal,
    expected_payment_note: str = "",
    consumed_message_ids: set[str],
    unread_only: bool = True,
) -> PaymentParserResult:
    normalized_confirm_time = confirm_pressed_at_utc.astimezone(timezone.utc)
    normalized_expected_amount = _normalize_decimal(expected_amount)
    normalized_expected_payment_note = expected_payment_note.strip().upper()
    now_utc = datetime.now(timezone.utc)
    default_result = _build_default_result("no candidate messages found")
    if normalized_expected_payment_note:
        default_result["expected_payment_note"] = normalized_expected_payment_note

    try:
        gmail_service = _build_gmail_service()
        parsed_messages = _fetch_recent_candidates(
            gmail_service,
            unread_only=unread_only,
        )
    except ValueError as exc:
        default_result["reason"] = "gmail api credentials missing"
        LOGGER.error(
            "payment_parser_setup_failed reason=%r error=%s",
            default_result["reason"],
            exc,
        )
        return default_result
    except HttpError:
        default_result["reason"] = "gmail api request failed"
        LOGGER.exception(
            "payment_parser_fetch_failed reason=%r",
            default_result["reason"],
        )
        return default_result
    except Exception:
        default_result["reason"] = "gmail api request failed"
        LOGGER.exception(
            "payment_parser_fetch_failed reason=%r",
            default_result["reason"],
        )
        return default_result

    evaluations = [
        _evaluate_candidate(
            parsed_message,
            confirm_pressed_at_utc=normalized_confirm_time,
            expected_amount=normalized_expected_amount,
            expected_payment_note=normalized_expected_payment_note,
            consumed_message_ids=consumed_message_ids,
            now_utc=now_utc,
        )
        for parsed_message in parsed_messages
    ]
    for evaluation in evaluations:
        _log_candidate_decision(evaluation)

    selected_result = _select_best_valid_candidate(evaluations)
    if selected_result is None:
        selected_result = _select_best_rejection(
            evaluations,
            expected_payment_note=normalized_expected_payment_note,
        )

    LOGGER.info(
        "payment_parser_resolution gmail_message_id=%s sender_address=%r sender_domain=%r amount_candidates=%s selected_amount=%r currency=%r forwarding_flags=%s timestamp_in_window=%s auth_summary=%r final_decision=%s rejection_reason=%r",
        selected_result.get("gmail_message_id"),
        selected_result.get("from_address"),
        selected_result.get("from_domain"),
        selected_result.get("amount_candidates", []),
        selected_result.get("amount"),
        selected_result.get("currency"),
        selected_result.get("forwarding_flags", []),
        selected_result.get("timestamp_in_window"),
        selected_result.get("auth_summary"),
        selected_result.get("matched"),
        None if selected_result.get("matched") else selected_result.get("reason"),
    )
    return selected_result


__all__ = [
    "PAYMENT_PARSER_EXPECTED_AMOUNT",
    "check_payment_email",
]
