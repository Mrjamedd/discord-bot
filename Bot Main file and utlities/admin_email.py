from __future__ import annotations

import logging
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Sequence

from config import (
    ADMIN_EMAIL_RECIPIENTS,
    SMTP_HOST,
    SMTP_PASSWORD_ENV,
    SMTP_PORT,
    SMTP_SENDER_ADDRESS,
    SMTP_TIMEOUT_SECONDS,
)


@dataclass(frozen=True)
class AdminEmailSettings:
    recipients: tuple[str, ...]
    sender_address: str
    smtp_host: str
    smtp_port: int
    smtp_password: str
    timeout_seconds: int

    def configuration_error(self) -> str | None:
        if not self.recipients:
            return "no admin email recipients configured"
        if not self.sender_address:
            return "no SMTP sender address configured"
        if not self.smtp_host:
            return "no SMTP host configured"
        if self.smtp_port <= 0:
            return "invalid SMTP port configured"
        if not self.smtp_password:
            return f"{SMTP_PASSWORD_ENV} is not set"
        if self.timeout_seconds <= 0:
            return "invalid SMTP timeout configured"
        return None


def load_admin_email_settings() -> AdminEmailSettings:
    return AdminEmailSettings(
        recipients=ADMIN_EMAIL_RECIPIENTS,
        sender_address=SMTP_SENDER_ADDRESS,
        smtp_host=SMTP_HOST,
        smtp_port=SMTP_PORT,
        smtp_password=(os.getenv(SMTP_PASSWORD_ENV) or "").strip(),
        timeout_seconds=SMTP_TIMEOUT_SECONDS,
    )


class AdminEmailNotifier:
    def __init__(
        self,
        logger: logging.Logger,
        settings: AdminEmailSettings | None = None,
    ) -> None:
        self.logger = logger
        self.settings = settings or load_admin_email_settings()

    def configuration_error(self) -> str | None:
        return self.settings.configuration_error()

    def is_configured(self) -> bool:
        return self.configuration_error() is None

    def send_email(
        self,
        *,
        subject: str,
        body: str,
        notification_type: str,
        recipients: Sequence[str] | None = None,
    ) -> bool:
        resolved_recipients = tuple(recipients or self.settings.recipients)
        configuration_error = AdminEmailSettings(
            recipients=resolved_recipients,
            sender_address=self.settings.sender_address,
            smtp_host=self.settings.smtp_host,
            smtp_port=self.settings.smtp_port,
            smtp_password=self.settings.smtp_password,
            timeout_seconds=self.settings.timeout_seconds,
        ).configuration_error()
        if configuration_error is not None:
            self.logger.warning(
                "admin_email_skipped notification_type=%s reason=%s subject=%r",
                notification_type,
                configuration_error,
                subject,
            )
            return False

        message = EmailMessage()
        message["From"] = self.settings.sender_address
        message["To"] = ", ".join(resolved_recipients)
        message["Subject"] = subject
        message.set_content(body)

        try:
            with smtplib.SMTP(
                self.settings.smtp_host,
                self.settings.smtp_port,
                timeout=self.settings.timeout_seconds,
            ) as smtp_client:
                smtp_client.ehlo()
                smtp_client.starttls()
                smtp_client.ehlo()
                smtp_client.login(
                    self.settings.sender_address,
                    self.settings.smtp_password,
                )
                smtp_client.send_message(message)
        except Exception:
            self.logger.exception(
                "admin_email_send_failed notification_type=%s subject=%r recipients=%s smtp_host=%s smtp_port=%s",
                notification_type,
                subject,
                resolved_recipients,
                self.settings.smtp_host,
                self.settings.smtp_port,
            )
            return False

        self.logger.info(
            "admin_email_sent notification_type=%s subject=%r recipients=%s smtp_host=%s smtp_port=%s",
            notification_type,
            subject,
            resolved_recipients,
            self.settings.smtp_host,
            self.settings.smtp_port,
        )
        return True


__all__ = [
    "AdminEmailNotifier",
    "AdminEmailSettings",
    "load_admin_email_settings",
]
