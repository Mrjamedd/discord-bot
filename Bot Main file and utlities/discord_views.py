from __future__ import annotations

from typing import TYPE_CHECKING

import discord

from config import (
    PAYMENT_BUTTON_CUSTOM_ID,
    PAYMENT_PLATFORM_BUTTON_CUSTOM_ID_PREFIX,
    SUPPORT_TICKET_BUTTON_CUSTOM_ID,
    TICKET_BUTTON_CUSTOM_ID,
)
from models import PaymentPlatform
from ticketing import PAYMENT_PLATFORMS

if TYPE_CHECKING:
    from bot import DiscordPurchaseBot


def payment_platform_button_custom_id(platform_key: str) -> str:
    return f"{PAYMENT_PLATFORM_BUTTON_CUSTOM_ID_PREFIX}:{platform_key}"


class PaymentPlatformButton(discord.ui.Button["PaymentPlatformSelectionView"]):
    def __init__(
        self,
        *,
        bot: "DiscordPurchaseBot",
        platform: PaymentPlatform,
    ) -> None:
        super().__init__(
            label=platform.label,
            style=discord.ButtonStyle.primary,
            custom_id=payment_platform_button_custom_id(platform.key),
        )
        self.bot = bot
        self.platform_key = platform.key

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.bot.handle_payment_platform_button(
            interaction,
            self.platform_key,
        )


class BotBoundView(discord.ui.View):
    def __init__(self, bot: "DiscordPurchaseBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: discord.ui.Item[discord.ui.View],
    ) -> None:
        channel = (
            interaction.channel if isinstance(interaction.channel, discord.TextChannel) else None
        )
        await self.bot.report_purchase_flow_exception(
            event_type="purchase_interaction_exception",
            trigger="interaction_callback",
            error=error,
            channel=channel,
            interaction=interaction,
            button_custom_id=getattr(item, "custom_id", None),
            failure_reason=f"{type(item).__name__} callback raised an unexpected exception",
            details={
                "view_class": type(self).__name__,
                "item_class": type(item).__name__,
            },
        )


class TicketLauncherView(BotBoundView):
    def __init__(self, bot: "DiscordPurchaseBot") -> None:
        super().__init__(bot)

    @discord.ui.button(
        label="Open Ticket",
        style=discord.ButtonStyle.primary,
        custom_id=TICKET_BUTTON_CUSTOM_ID,
    )
    async def open_ticket(
        self,
        interaction: discord.Interaction,
        _: discord.ui.Button["TicketLauncherView"],
    ) -> None:
        await self.bot.handle_ticket_button(interaction)

class SupportTicketLauncherView(BotBoundView):
    def __init__(self, bot: "DiscordPurchaseBot") -> None:
        super().__init__(bot)

    @discord.ui.button(
        label="Open Support Ticket",
        style=discord.ButtonStyle.primary,
        custom_id=SUPPORT_TICKET_BUTTON_CUSTOM_ID,
    )
    async def open_support_ticket(
        self,
        interaction: discord.Interaction,
        _: discord.ui.Button["SupportTicketLauncherView"],
    ) -> None:
        await self.bot.handle_support_ticket_button(interaction)


class PaymentPlatformSelectionView(BotBoundView):
    def __init__(self, bot: "DiscordPurchaseBot") -> None:
        super().__init__(bot)
        for platform in PAYMENT_PLATFORMS:
            self.add_item(PaymentPlatformButton(bot=bot, platform=platform))


class PaymentConfirmationView(BotBoundView):
    def __init__(self, bot: "DiscordPurchaseBot") -> None:
        super().__init__(bot)

    @discord.ui.button(
        label="Confirm Payment",
        style=discord.ButtonStyle.success,
        custom_id=PAYMENT_BUTTON_CUSTOM_ID,
    )
    async def confirm_payment(
        self,
        interaction: discord.Interaction,
        _: discord.ui.Button["PaymentConfirmationView"],
    ) -> None:
        await self.bot.handle_payment_button(interaction)
