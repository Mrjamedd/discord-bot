from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast
from uuid import uuid4

import discord

from Email_Parser import check_payment_email
from config import (
    PAYMENT_BUTTON_CUSTOM_ID,
    PAYMENT_CHECK_DELAY_SECONDS,
    PAYMENT_PARSER_EXPECTED_AMOUNT,
    PURCHASE_SYNC_RETRY_INTERVAL_SECONDS,
    SUPPORT_MODERATOR_ROLE_ID,
    SUPPORT_TICKET_BUTTON_CUSTOM_ID,
    SUPPORT_TICKET_CATEGORY_ID,
    SUPPORT_TICKET_CHANNEL_MESSAGE,
    SUPPORT_TICKET_PANEL_CHANNEL_ID,
    TICKET_CATEGORY_ID,
    TICKET_BUTTON_CUSTOM_ID,
    TICKET_PANEL_CHANNEL_ID,
)
from discord_views import (
    PaymentConfirmationView,
    PaymentPlatformSelectionView,
    SupportTicketLauncherView,
    TicketLauncherView,
    payment_platform_button_custom_id,
)
from models import BotState, PaymentParserResult, PurchaseRecord, ScriptProduct, TicketRecord
from purchase_logger import PurchaseLogger
from state_manager import (
    fresh_ticket_record,
    get_payment_parser_state,
    get_ticket_record,
    load_state,
    purge_consumed_message_ids,
    record_consumed_message_id,
    save_state,
)
from ticketing import (
    CONFIRM_SELECTION_RESPONSE,
    PAYMENT_PLATFORMS,
    TICKET_STAGE_AWAITING_CONFIRMATION,
    TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
    TICKET_STAGE_AWAITING_PAYMENT,
    TICKET_STAGE_AWAITING_SELECTION,
    TICKET_STAGE_COMPLETED,
    TICKET_STAGE_PAYMENT_PENDING,
    UNSET,
    build_payment_instruction_message,
    build_payment_platform_prompt_message,
    build_script_confirmation_message,
    build_script_delivery_file,
    build_support_ticket_channel_name,
    build_support_ticket_panel_message,
    build_ticket_channel_name,
    build_ticket_panel_message,
    build_ticket_retry_message,
    build_ticket_store_message,
    find_script_product,
    generate_payment_note_code,
    get_payment_platform_by_key,
    get_script_product_by_key,
    message_is_selection_confirmation,
    ticket_owner_id_from_topic,
    ticket_owner_topic,
)
from utils import (
    message_has_component_custom_id,
    split_message,
    utc_timestamp,
)

class DiscordPurchaseBot(discord.Client):
    def __init__(
        self,
        *,
        logger: logging.Logger,
        purchase_logger: PurchaseLogger,
    ) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.logger = logger
        self.purchase_logger = purchase_logger
        self.response_allowed_mentions: discord.AllowedMentions = (
            discord.AllowedMentions.none()
        )
        self.support_ping_allowed_mentions = discord.AllowedMentions(
            everyone=False,
            users=False,
            roles=True,
            replied_user=False,
        )
        self.state: BotState = load_state()
        self.state_lock = asyncio.Lock()
        self.ticket_panel_lock = asyncio.Lock()
        self.ticket_creation_lock = asyncio.Lock()
        self.support_ticket_panel_lock = asyncio.Lock()
        self.support_ticket_creation_lock = asyncio.Lock()
        self.payment_check_tasks: set[asyncio.Task[None]] = set()
        self.pending_payment_check_channel_ids: set[int] = set()
        self.purchase_sync_lock = asyncio.Lock()
        self.purchase_sync_retry_task: asyncio.Task[None] | None = None
        self.payment_parser_lock = asyncio.Lock()

    def build_ticket_panel_view(self) -> TicketLauncherView:
        return TicketLauncherView(self)

    def build_support_ticket_panel_view(self) -> SupportTicketLauncherView:
        return SupportTicketLauncherView(self)

    def build_payment_platform_selection_view(self) -> PaymentPlatformSelectionView:
        return PaymentPlatformSelectionView(self)

    def build_payment_confirmation_view(self) -> PaymentConfirmationView:
        return PaymentConfirmationView(self)

    def cleanup_payment_task(self, completed: asyncio.Future[None]) -> None:
        if isinstance(completed, asyncio.Task):
            task = cast(asyncio.Task[None], completed)
            self.payment_check_tasks.discard(task)
            if task.cancelled():
                return
            try:
                task.result()
            except Exception:
                self.logger.exception(
                    "payment_check_task_failed timestamp=%s",
                    utc_timestamp(),
                )

    async def retry_pending_purchase_logs(self) -> None:
        async with self.purchase_sync_lock:
            try:
                synced_records, total_records = await asyncio.to_thread(
                    self.purchase_logger.retry_pending_records
                )
            except Exception:
                self.logger.exception(
                    "purchase_sync_retry_failed timestamp=%s",
                    utc_timestamp(),
                )
                return

            if total_records > 0:
                self.logger.info(
                    "purchase_sync_retry_completed timestamp=%s synced_records=%s total_records=%s remaining_records=%s",
                    utc_timestamp(),
                    synced_records,
                    total_records,
                    total_records - synced_records,
                )

    async def purchase_sync_retry_loop(self) -> None:
        try:
            while not self.is_closed():
                await self.retry_pending_purchase_logs()
                await asyncio.sleep(PURCHASE_SYNC_RETRY_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            return

    async def get_authoritative_ticket_owner_id(
        self,
        channel: discord.TextChannel,
        *,
        fallback_owner_id: int | None = None,
    ) -> int | None:
        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        state_owner_id = ticket_record.get("owner_id")
        authoritative_owner_id = (
            state_owner_id
            if isinstance(state_owner_id, int) and not isinstance(state_owner_id, bool)
            else None
        )
        topic_owner_id = ticket_owner_id_from_topic(channel.topic)

        if authoritative_owner_id is not None:
            if topic_owner_id is not None and topic_owner_id != authoritative_owner_id:
                self.logger.warning(
                    "ticket_owner_topic_mismatch channel_id=%s state_owner_id=%s topic_owner_id=%s timestamp=%s",
                    channel.id,
                    authoritative_owner_id,
                    topic_owner_id,
                    utc_timestamp(),
                )
            return authoritative_owner_id

        if topic_owner_id is not None:
            await self.update_ticket_record(channel.id, owner_id=topic_owner_id)
            return topic_owner_id

        overwrite_owner_id = self.infer_ticket_owner_id_from_overwrites(channel)
        if overwrite_owner_id is not None:
            await self.update_ticket_record(channel.id, owner_id=overwrite_owner_id)
            return overwrite_owner_id

        if fallback_owner_id is not None:
            self.logger.warning(
                "ticket_owner_fallback_used channel_id=%s fallback_owner_id=%s timestamp=%s",
                channel.id,
                fallback_owner_id,
                utc_timestamp(),
            )
            await self.update_ticket_record(channel.id, owner_id=fallback_owner_id)
            return fallback_owner_id

        return None

    async def get_ticket_record_snapshot(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
    ) -> TicketRecord:
        async with self.state_lock:
            record_key = str(channel_id)
            tickets = self.state.get("tickets")
            if tickets is None:
                tickets = {}
                self.state["tickets"] = tickets

            changed = False
            record = tickets.get(record_key)
            if record is None:
                record = fresh_ticket_record(owner_id)
                tickets[record_key] = record
                changed = True
            elif owner_id is not None and record.get("owner_id") is None:
                record["owner_id"] = owner_id
                changed = True

            if changed:
                self.persist_state()
            return cast(TicketRecord, dict(record))

    async def update_ticket_record(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
        selected_script_key: object = UNSET,
        payment_platform_key: object = UNSET,
        payment_note_code: object = UNSET,
        stage: str | None = None,
    ) -> TicketRecord:
        async with self.state_lock:
            record = get_ticket_record(
                self.state,
                str(channel_id),
                owner_id=owner_id,
            )
            if stage is not None:
                record["stage"] = stage
            if selected_script_key is not UNSET:
                record["selected_script_key"] = cast(str | None, selected_script_key)
            if payment_platform_key is not UNSET:
                record["payment_platform_key"] = cast(
                    str | None,
                    payment_platform_key,
                )
            if payment_note_code is not UNSET:
                record["payment_note_code"] = cast(str | None, payment_note_code)
            self.persist_state()
            return cast(TicketRecord, dict(record))

    async def ensure_payment_note_code(
        self,
        channel_id: int,
        *,
        owner_id: int | None = None,
    ) -> str:
        ticket_record = await self.get_ticket_record_snapshot(channel_id)
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        if payment_note_code:
            return payment_note_code

        payment_note_code = generate_payment_note_code()
        await self.update_ticket_record(
            channel_id,
            owner_id=owner_id,
            payment_note_code=payment_note_code,
        )
        return payment_note_code

    async def setup_hook(self) -> None:
        self.add_view(self.build_ticket_panel_view())
        self.add_view(self.build_support_ticket_panel_view())
        self.add_view(self.build_payment_platform_selection_view())
        self.add_view(self.build_payment_confirmation_view())
        self.logger.info("ticket_view_registered custom_id=%s", TICKET_BUTTON_CUSTOM_ID)
        self.logger.info(
            "support_ticket_view_registered custom_id=%s",
            SUPPORT_TICKET_BUTTON_CUSTOM_ID,
        )
        for platform in PAYMENT_PLATFORMS:
            self.logger.info(
                "payment_platform_view_registered custom_id=%s platform=%s",
                payment_platform_button_custom_id(platform.key),
                platform.key,
            )
        self.logger.info(
            "payment_view_registered custom_id=%s",
            PAYMENT_BUTTON_CUSTOM_ID,
        )

    async def on_ready(self) -> None:
        if self.user is None:
            return

        self.logger.info(
            "bot_ready user_id=%s username=%r timestamp=%s",
            self.user.id,
            str(self.user),
            utc_timestamp(),
        )
        print(f"Logged in as {self.user} ({self.user.id})")
        await self.ensure_ticket_panel()
        await self.ensure_support_ticket_panel()
        await self.retry_pending_purchase_logs()
        if self.purchase_sync_retry_task is None or self.purchase_sync_retry_task.done():
            self.purchase_sync_retry_task = asyncio.create_task(
                self.purchase_sync_retry_loop()
            )

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        interaction_type = getattr(interaction.type, "name", str(interaction.type))
        custom_id: str | None = None
        if isinstance(interaction.data, dict):
            custom_id_value = cast(object, interaction.data.get("custom_id"))
            if isinstance(custom_id_value, str):
                custom_id = custom_id_value

        self.logger.info(
            "interaction_received type=%s custom_id=%r user_id=%s channel_id=%s timestamp=%s",
            interaction_type,
            custom_id,
            getattr(interaction.user, "id", None),
            getattr(interaction.channel, "id", None),
            utc_timestamp(),
        )

    async def get_ticket_panel_channel(self) -> discord.TextChannel | None:
        channel = self.get_channel(TICKET_PANEL_CHANNEL_ID)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(TICKET_PANEL_CHANNEL_ID)
        except discord.DiscordException:
            self.logger.exception(
                "ticket_panel_channel_fetch_failed channel_id=%s timestamp=%s",
                TICKET_PANEL_CHANNEL_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.TextChannel):
            return fetched_channel

        self.logger.error(
            "ticket_panel_channel_invalid_type channel_id=%s timestamp=%s",
            TICKET_PANEL_CHANNEL_ID,
            utc_timestamp(),
        )
        return None

    async def get_support_ticket_panel_channel(self) -> discord.TextChannel | None:
        channel = self.get_channel(SUPPORT_TICKET_PANEL_CHANNEL_ID)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(SUPPORT_TICKET_PANEL_CHANNEL_ID)
        except discord.DiscordException:
            self.logger.exception(
                "support_ticket_panel_channel_fetch_failed channel_id=%s timestamp=%s",
                SUPPORT_TICKET_PANEL_CHANNEL_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.TextChannel):
            return fetched_channel

        self.logger.error(
            "support_ticket_panel_channel_invalid_type channel_id=%s timestamp=%s",
            SUPPORT_TICKET_PANEL_CHANNEL_ID,
            utc_timestamp(),
        )
        return None

    async def get_ticket_category(self) -> discord.CategoryChannel | None:
        channel = self.get_channel(TICKET_CATEGORY_ID)
        if isinstance(channel, discord.CategoryChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(TICKET_CATEGORY_ID)
        except discord.DiscordException:
            self.logger.exception(
                "ticket_category_fetch_failed category_id=%s timestamp=%s",
                TICKET_CATEGORY_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.CategoryChannel):
            return fetched_channel

        self.logger.error(
            "ticket_category_invalid_type category_id=%s timestamp=%s",
            TICKET_CATEGORY_ID,
            utc_timestamp(),
        )
        return None

    async def get_support_ticket_category(self) -> discord.CategoryChannel | None:
        channel = self.get_channel(SUPPORT_TICKET_CATEGORY_ID)
        if isinstance(channel, discord.CategoryChannel):
            return channel

        try:
            fetched_channel = await self.fetch_channel(SUPPORT_TICKET_CATEGORY_ID)
        except discord.DiscordException:
            self.logger.exception(
                "support_ticket_category_fetch_failed category_id=%s timestamp=%s",
                SUPPORT_TICKET_CATEGORY_ID,
                utc_timestamp(),
            )
            return None

        if isinstance(fetched_channel, discord.CategoryChannel):
            return fetched_channel

        self.logger.error(
            "support_ticket_category_invalid_type category_id=%s timestamp=%s",
            SUPPORT_TICKET_CATEGORY_ID,
            utc_timestamp(),
        )
        return None

    async def ensure_ticket_panel(self) -> None:
        channel = await self.get_ticket_panel_channel()
        if channel is None:
            return

        async with self.ticket_panel_lock:
            panel_message_text = build_ticket_panel_message()
            message_id = self.state.get("ticket_panel_message_id")
            stored_message: discord.Message | None = None

            if isinstance(message_id, int):
                try:
                    stored_message = await channel.fetch_message(message_id)
                except discord.NotFound:
                    stored_message = None
                except discord.DiscordException:
                    self.logger.exception(
                        "ticket_panel_message_fetch_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        message_id,
                        utc_timestamp(),
                    )
                    return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
                and message_has_component_custom_id(
                    stored_message,
                    TICKET_BUTTON_CUSTOM_ID,
                )
                and stored_message.content == panel_message_text
            ):
                return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
            ):
                try:
                    await stored_message.edit(
                        content=panel_message_text,
                        view=self.build_ticket_panel_view(),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    return
                except discord.DiscordException:
                    self.logger.exception(
                        "ticket_panel_message_edit_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        stored_message.id,
                        utc_timestamp(),
                    )
                    return

            if stored_message is not None:
                self.logger.warning(
                    "ticket_panel_message_unexpected_author channel_id=%s message_id=%s timestamp=%s",
                    channel.id,
                    stored_message.id,
                    utc_timestamp(),
                )
                return

            try:
                panel_message = await channel.send(
                    panel_message_text,
                    view=self.build_ticket_panel_view(),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "ticket_panel_message_send_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )
                return

            async with self.state_lock:
                self.state["ticket_panel_message_id"] = panel_message.id
                self.persist_state()

    async def ensure_support_ticket_panel(self) -> None:
        channel = await self.get_support_ticket_panel_channel()
        if channel is None:
            return

        async with self.support_ticket_panel_lock:
            panel_message_text = build_support_ticket_panel_message()
            message_id = self.state.get("support_ticket_panel_message_id")
            stored_message: discord.Message | None = None

            if isinstance(message_id, int):
                try:
                    stored_message = await channel.fetch_message(message_id)
                except discord.NotFound:
                    stored_message = None
                except discord.DiscordException:
                    self.logger.exception(
                        "support_ticket_panel_message_fetch_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        message_id,
                        utc_timestamp(),
                    )
                    return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
                and message_has_component_custom_id(
                    stored_message,
                    SUPPORT_TICKET_BUTTON_CUSTOM_ID,
                )
                and stored_message.content == panel_message_text
            ):
                return

            if (
                stored_message is not None
                and self.user is not None
                and stored_message.author.id == self.user.id
            ):
                try:
                    await stored_message.edit(
                        content=panel_message_text,
                        view=self.build_support_ticket_panel_view(),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    return
                except discord.DiscordException:
                    self.logger.exception(
                        "support_ticket_panel_message_edit_failed channel_id=%s message_id=%s timestamp=%s",
                        channel.id,
                        stored_message.id,
                        utc_timestamp(),
                    )
                    return

            if stored_message is not None:
                self.logger.warning(
                    "support_ticket_panel_message_unexpected_author channel_id=%s message_id=%s timestamp=%s",
                    channel.id,
                    stored_message.id,
                    utc_timestamp(),
                )
                return

            try:
                panel_message = await channel.send(
                    panel_message_text,
                    view=self.build_support_ticket_panel_view(),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "support_ticket_panel_message_send_failed channel_id=%s timestamp=%s",
                    channel.id,
                    utc_timestamp(),
                )
                return

            async with self.state_lock:
                self.state["support_ticket_panel_message_id"] = panel_message.id
                self.persist_state()

    def is_purchase_ticket_channel(self, channel: discord.abc.Messageable) -> bool:
        return (
            isinstance(channel, discord.TextChannel)
            and channel.category_id == TICKET_CATEGORY_ID
            and channel.name.startswith("ticket-")
        )

    def is_support_ticket_channel(self, channel: discord.abc.Messageable) -> bool:
        return (
            isinstance(channel, discord.TextChannel)
            and channel.category_id == SUPPORT_TICKET_CATEGORY_ID
            and channel.name.startswith("support-ticket-")
        )

    def infer_ticket_owner_id_from_overwrites(
        self,
        channel: discord.TextChannel,
    ) -> int | None:
        bot_user_id = self.user.id if self.user is not None else None
        candidate_owner_ids: list[int] = []

        for target, overwrite in channel.overwrites.items():
            member_id: int | None = None
            if isinstance(target, discord.Member):
                member_id = target.id
            elif isinstance(target, discord.Object):
                member = channel.guild.get_member(target.id)
                if member is not None:
                    member_id = member.id

            if member_id is None or member_id == bot_user_id:
                continue
            if overwrite.view_channel is False:
                continue
            if member_id not in candidate_owner_ids:
                candidate_owner_ids.append(member_id)

        if len(candidate_owner_ids) == 1:
            return candidate_owner_ids[0]

        if len(candidate_owner_ids) > 1:
            self.logger.warning(
                "ticket_owner_permission_overwrites_ambiguous channel_id=%s candidate_owner_ids=%s timestamp=%s",
                channel.id,
                candidate_owner_ids,
                utc_timestamp(),
            )
        return None

    def find_existing_ticket_channel(
        self,
        category: discord.CategoryChannel,
        user_id: int,
    ) -> discord.TextChannel | None:
        expected_topic = ticket_owner_topic(user_id)
        for channel in category.text_channels:
            if (
                channel.topic == expected_topic
                or self.infer_ticket_owner_id_from_overwrites(channel) == user_id
            ):
                return channel
        return None

    def get_ticket_support_roles(self, guild: discord.Guild) -> list[discord.Role]:
        support_roles: list[discord.Role] = []
        for role in guild.roles:
            permissions = role.permissions
            if (
                permissions.administrator
                or permissions.manage_guild
                or permissions.manage_channels
                or permissions.manage_messages
                or permissions.moderate_members
                or permissions.kick_members
                or permissions.ban_members
            ):
                support_roles.append(role)
        return support_roles

    def build_unique_ticket_channel_name(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> str:
        base_name = build_ticket_channel_name(member.display_name or member.name)
        return self.build_unique_channel_name(category, member.id, base_name)

    def build_unique_support_ticket_channel_name(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> str:
        base_name = build_support_ticket_channel_name(member.display_name or member.name)
        return self.build_unique_channel_name(category, member.id, base_name)

    def build_unique_channel_name(
        self,
        category: discord.CategoryChannel,
        user_id: int,
        base_name: str,
    ) -> str:
        existing_names = {channel.name for channel in category.text_channels}
        if base_name not in existing_names:
            return base_name

        base_suffix = str(user_id)[-4:]
        for attempt in range(100):
            suffix = base_suffix if attempt == 0 else f"{base_suffix}-{attempt + 1}"
            trimmed_base = base_name[: max(1, 99 - len(suffix) - 1)]
            candidate_name = f"{trimmed_base}-{suffix}"
            if candidate_name not in existing_names:
                return candidate_name

        fallback_suffix = str(user_id)
        trimmed_base = base_name[: max(1, 99 - len(fallback_suffix) - 1)]
        return f"{trimmed_base}-{fallback_suffix}"

    def get_support_moderator_role(self, guild: discord.Guild) -> discord.Role | None:
        return guild.get_role(SUPPORT_MODERATOR_ROLE_ID)

    async def create_ticket_channel(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> discord.TextChannel:
        bot_member = category.guild.me
        if bot_member is None and self.user is not None:
            bot_member = category.guild.get_member(self.user.id)
        if bot_member is None:
            raise RuntimeError("Unable to resolve the bot member for ticket permissions.")

        overwrites: dict[
            discord.Role | discord.Member | discord.Object,
            discord.PermissionOverwrite,
        ] = {
            category.guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            ),
        }

        for role in self.get_ticket_support_roles(category.guild):
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        channel_name = self.build_unique_ticket_channel_name(category, member)
        channel = await category.guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=ticket_owner_topic(member.id),
            reason=f"Purchase ticket opened by {member} ({member.id})",
        )
        await channel.send(
            build_ticket_store_message(member.display_name or member.name),
            allowed_mentions=self.response_allowed_mentions,
        )
        await self.update_ticket_record(
            channel.id,
            owner_id=member.id,
            selected_script_key=None,
            stage=TICKET_STAGE_AWAITING_SELECTION,
        )
        return channel

    async def create_support_ticket_channel(
        self,
        category: discord.CategoryChannel,
        member: discord.Member,
    ) -> discord.TextChannel:
        bot_member = category.guild.me
        if bot_member is None and self.user is not None:
            bot_member = category.guild.get_member(self.user.id)
        if bot_member is None:
            raise RuntimeError("Unable to resolve the bot member for ticket permissions.")

        overwrites: dict[
            discord.Role | discord.Member | discord.Object,
            discord.PermissionOverwrite,
        ] = {
            category.guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            ),
        }

        for role in self.get_ticket_support_roles(category.guild):
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        moderator_role = self.get_support_moderator_role(category.guild)
        if moderator_role is None:
            self.logger.warning(
                "support_moderator_role_missing guild_id=%s role_id=%s timestamp=%s",
                category.guild.id,
                SUPPORT_MODERATOR_ROLE_ID,
                utc_timestamp(),
            )
        else:
            overwrites[moderator_role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

        channel_name = self.build_unique_support_ticket_channel_name(category, member)
        channel = await category.guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=ticket_owner_topic(member.id),
            reason=f"Support ticket opened by {member} ({member.id})",
        )
        await channel.send(
            SUPPORT_TICKET_CHANNEL_MESSAGE,
            allowed_mentions=self.support_ping_allowed_mentions,
        )
        return channel

    async def handle_ticket_button(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Tickets can only be opened inside a server.",
                ephemeral=True,
            )
            return

        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None:
            member = guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                "I couldn't resolve your server membership. Please try again.",
                ephemeral=True,
            )
            return

        category = await self.get_ticket_category()
        if category is None or category.guild.id != guild.id:
            await interaction.response.send_message(
                "The ticket category is unavailable right now. Please contact a moderator.",
                ephemeral=True,
            )
            return

        async with self.ticket_creation_lock:
            existing_channel = self.find_existing_ticket_channel(category, member.id)
            if existing_channel is not None:
                await interaction.response.send_message(
                    f"You already have an open ticket: {existing_channel.mention}",
                    ephemeral=True,
                )
                return

            try:
                ticket_channel = await self.create_ticket_channel(category, member)
            except Exception:
                self.logger.exception(
                    "ticket_channel_create_failed guild_id=%s user_id=%s timestamp=%s",
                    guild.id,
                    member.id,
                    utc_timestamp(),
                )
                await interaction.response.send_message(
                    "I couldn't create your ticket right now. Please try again shortly.",
                    ephemeral=True,
                )
                return

        self.logger.info(
            "ticket_channel_created guild_id=%s channel_id=%s user_id=%s timestamp=%s",
            guild.id,
            ticket_channel.id,
            member.id,
            utc_timestamp(),
        )
        await interaction.response.send_message(
            f"Your ticket is ready: {ticket_channel.mention}",
            ephemeral=True,
        )

    async def handle_support_ticket_button(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Tickets can only be opened inside a server.",
                ephemeral=True,
            )
            return

        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None:
            member = guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message(
                "I couldn't resolve your server membership. Please try again.",
                ephemeral=True,
            )
            return

        category = await self.get_support_ticket_category()
        if category is None or category.guild.id != guild.id:
            await interaction.response.send_message(
                "The support ticket category is unavailable right now. Please contact a moderator.",
                ephemeral=True,
            )
            return

        async with self.support_ticket_creation_lock:
            existing_channel = self.find_existing_ticket_channel(category, member.id)
            if existing_channel is not None:
                await interaction.response.send_message(
                    f"You already have an open support ticket: {existing_channel.mention}",
                    ephemeral=True,
                )
                return

            try:
                ticket_channel = await self.create_support_ticket_channel(category, member)
            except Exception:
                self.logger.exception(
                    "support_ticket_channel_create_failed guild_id=%s user_id=%s timestamp=%s",
                    guild.id,
                    member.id,
                    utc_timestamp(),
                )
                await interaction.response.send_message(
                    "I couldn't create your support ticket right now. Please try again shortly.",
                    ephemeral=True,
                )
                return

        self.logger.info(
            "support_ticket_channel_created guild_id=%s channel_id=%s user_id=%s timestamp=%s",
            guild.id,
            ticket_channel.id,
            member.id,
            utc_timestamp(),
        )
        await interaction.response.send_message(
            f"Your support ticket is ready: {ticket_channel.mention}",
            ephemeral=True,
        )

    async def handle_payment_button(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel) or not self.is_purchase_ticket_channel(channel):
            await interaction.response.send_message(
                "This button only works inside a ticket channel.",
                ephemeral=True,
            )
            return

        owner_id = await self.get_authoritative_ticket_owner_id(channel)
        if owner_id is None:
            await interaction.response.send_message(
                "I couldn't verify the ticket owner from saved state. Please contact support.",
                ephemeral=True,
            )
            return

        if owner_id is not None and interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Only the ticket owner can confirm payment here.",
                ephemeral=True,
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(
            channel.id,
        )
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )

        if ticket_stage == TICKET_STAGE_COMPLETED:
            await interaction.response.send_message(
                "Payment has already been confirmed for this ticket.",
                ephemeral=True,
            )
            return

        if ticket_stage == TICKET_STAGE_AWAITING_PAYMENT_PLATFORM:
            await interaction.response.send_message(
                "Choose a payment platform first before checking payment.",
                ephemeral=True,
            )
            return

        if selected_product is None or ticket_stage not in {
            TICKET_STAGE_AWAITING_PAYMENT,
            TICKET_STAGE_PAYMENT_PENDING,
        }:
            await interaction.response.send_message(
                "Confirm your script selection first before checking payment.",
                ephemeral=True,
            )
            return

        if not payment_note_code:
            await interaction.response.send_message(
                "This ticket is missing its required payment note code. Choose the payment platform again to get the exact code before confirming payment.",
                ephemeral=True,
            )
            return

        if channel.id in self.pending_payment_check_channel_ids:
            await interaction.response.send_message(
                "A payment check is already running for this ticket.",
                ephemeral=True,
            )
            return

        await self.update_ticket_record(
            channel.id,
            owner_id=owner_id,
            stage=TICKET_STAGE_PAYMENT_PENDING,
        )
        self.pending_payment_check_channel_ids.add(channel.id)
        confirm_pressed_at_utc = datetime.now(timezone.utc)
        task = asyncio.create_task(
            self.run_payment_confirmation_check(
                channel,
                interaction.user.id,
                confirm_pressed_at_utc=confirm_pressed_at_utc,
            )
        )
        self.payment_check_tasks.add(task)
        task.add_done_callback(self.cleanup_payment_task)
        try:
            await interaction.response.send_message(
                (
                    "Payment check scheduled. "
                    f"I will attempt to confirm the payment in {PAYMENT_CHECK_DELAY_SECONDS} seconds."
                ),
                ephemeral=True,
            )
        except discord.DiscordException:
            self.logger.exception(
                "payment_check_acknowledgement_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                interaction.user.id,
                utc_timestamp(),
            )
            try:
                await channel.send(
                    (
                        "Payment check scheduled. "
                        f"I will attempt to confirm the payment in {PAYMENT_CHECK_DELAY_SECONDS} seconds."
                    ),
                    allowed_mentions=self.response_allowed_mentions,
                )
            except discord.DiscordException:
                self.logger.exception(
                    "payment_check_acknowledgement_fallback_failed channel_id=%s user_id=%s timestamp=%s",
                    channel.id,
                    interaction.user.id,
                    utc_timestamp(),
                )

    async def handle_payment_platform_button(
        self,
        interaction: discord.Interaction,
        platform_key: str,
    ) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel) or not self.is_purchase_ticket_channel(channel):
            await interaction.response.send_message(
                "This button only works inside a ticket channel.",
                ephemeral=True,
            )
            return

        owner_id = await self.get_authoritative_ticket_owner_id(channel)
        if owner_id is None:
            await interaction.response.send_message(
                "I couldn't verify the ticket owner from saved state. Please contact support.",
                ephemeral=True,
            )
            return

        if interaction.user.id != owner_id:
            await interaction.response.send_message(
                "Only the ticket owner can choose the payment platform here.",
                ephemeral=True,
            )
            return

        selected_platform = get_payment_platform_by_key(platform_key)
        if selected_platform is None:
            await interaction.response.send_message(
                "That payment platform isn't available right now.",
                ephemeral=True,
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )

        if ticket_stage == TICKET_STAGE_COMPLETED:
            await interaction.response.send_message(
                "Payment has already been confirmed for this ticket.",
                ephemeral=True,
            )
            return

        if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
            await interaction.response.send_message(
                "A payment check is already running for this ticket.",
                ephemeral=True,
            )
            return

        if selected_product is None or ticket_stage not in {
            TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
            TICKET_STAGE_AWAITING_PAYMENT,
        }:
            await interaction.response.send_message(
                "Confirm your script selection first before choosing a payment platform.",
                ephemeral=True,
            )
            return

        payment_note_code = await self.ensure_payment_note_code(
            channel.id,
            owner_id=owner_id,
        )

        try:
            await interaction.response.send_message(
                build_payment_instruction_message(
                    selected_product,
                    selected_platform,
                    payment_note_code,
                ),
                view=self.build_payment_confirmation_view(),
                allowed_mentions=self.response_allowed_mentions,
            )
        except discord.DiscordException:
            self.logger.exception(
                "ticket_payment_instructions_send_failed channel_id=%s user_id=%s platform=%s timestamp=%s",
                channel.id,
                interaction.user.id,
                selected_platform.key,
                utc_timestamp(),
            )
            if not interaction.response.is_done():
                try:
                    await interaction.response.send_message(
                        "I couldn't send the payment instructions right now. Please try again shortly.",
                        ephemeral=True,
                    )
                except discord.DiscordException:
                    pass
            return

        await self.update_ticket_record(
            channel.id,
            owner_id=owner_id,
            payment_platform_key=selected_platform.key,
            payment_note_code=payment_note_code,
            stage=TICKET_STAGE_AWAITING_PAYMENT,
        )
        self.logger.info(
            "ticket_payment_platform_selected channel_id=%s user_id=%s script=%s platform=%s timestamp=%s",
            channel.id,
            interaction.user.id,
            selected_product.key,
            selected_platform.key,
            utc_timestamp(),
        )

    async def run_payment_confirmation_check(
        self,
        channel: discord.TextChannel,
        user_id: int,
        *,
        confirm_pressed_at_utc: datetime,
    ) -> None:
        self.logger.info(
            "payment_check_scheduled channel_id=%s user_id=%s delay_seconds=%s timestamp=%s",
            channel.id,
            user_id,
            PAYMENT_CHECK_DELAY_SECONDS,
            utc_timestamp(),
        )

        ticket_record = await self.get_ticket_record_snapshot(channel.id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        parser_result: PaymentParserResult | None = None
        message_text = "payment check failed right now"

        try:
            await asyncio.sleep(PAYMENT_CHECK_DELAY_SECONDS)
            expected_amount = (
                Decimal(str(selected_product.price))
                if selected_product is not None
                else PAYMENT_PARSER_EXPECTED_AMOUNT
            )
            if not payment_note_code:
                parser_result = {
                    "matched": False,
                    "reason": "payment note code unavailable",
                }
            else:
                async with self.payment_parser_lock:
                    consumed_message_ids = await self.get_consumed_payment_message_ids_snapshot()
                    parser_result = await asyncio.to_thread(
                        check_payment_email,
                        confirm_pressed_at_utc=confirm_pressed_at_utc,
                        expected_amount=expected_amount,
                        expected_payment_note=payment_note_code,
                        consumed_message_ids=consumed_message_ids,
                    )
                    if parser_result.get("matched"):
                        gmail_message_id = cast(
                            str | None,
                            parser_result.get("gmail_message_id"),
                        )
                        if not gmail_message_id:
                            parser_result = {
                                **parser_result,
                                "matched": False,
                                "reason": "parser result missing message id",
                            }
                        else:
                            consumed_ok = await self.record_consumed_payment_message_id(
                                gmail_message_id
                            )
                            if not consumed_ok:
                                parser_result = {
                                    **parser_result,
                                    "matched": False,
                                    "reason": "payment confirmation could not be recorded safely",
                                }
            self.logger.info(
                "payment_check_completed channel_id=%s user_id=%s matched=%s reason=%r gmail_message_id=%s from_address=%r from_domain=%r amount=%r currency=%r received_timestamp_utc=%r auth_summary=%r forwarding_flags=%s timestamp=%s",
                channel.id,
                user_id,
                None if parser_result is None else parser_result.get("matched"),
                None if parser_result is None else parser_result.get("reason"),
                None if parser_result is None else parser_result.get("gmail_message_id"),
                None if parser_result is None else parser_result.get("from_address"),
                None if parser_result is None else parser_result.get("from_domain"),
                None if parser_result is None else parser_result.get("amount"),
                None if parser_result is None else parser_result.get("currency"),
                None if parser_result is None else parser_result.get("received_timestamp_utc"),
                None if parser_result is None else parser_result.get("auth_summary"),
                [] if parser_result is None else parser_result.get("forwarding_flags", []),
                utc_timestamp(),
            )
        except Exception:
            self.logger.exception(
                "payment_check_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                user_id,
                utc_timestamp(),
            )
            await self.update_ticket_record(
                channel.id,
                stage=TICKET_STAGE_AWAITING_PAYMENT,
            )
        finally:
            self.pending_payment_check_channel_ids.discard(channel.id)

        if parser_result is not None and parser_result.get("matched") is True:
            if selected_product is None:
                await self.update_ticket_record(
                    channel.id,
                    stage=TICKET_STAGE_PAYMENT_PENDING,
                )
                message_text = (
                    "Payment was confirmed, but I couldn't determine which script was "
                    "selected. Please contact support."
                )
            else:
                try:
                    await channel.send(
                        (
                            f"Payment confirmed for {selected_product.label}. "
                            f"Here is your `{selected_product.file_path.name}` file."
                        ),
                        file=build_script_delivery_file(selected_product),
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_COMPLETED,
                    )
                    await self.record_successful_purchase(
                        channel,
                        user_id,
                        selected_product,
                    )
                    return
                except FileNotFoundError:
                    self.logger.exception(
                        "ticket_script_file_missing channel_id=%s user_id=%s script=%s file_path=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        selected_product.file_path,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    message_text = (
                        "Payment was confirmed, but the delivery file is missing right "
                        "now. Please contact support."
                    )
                except (OSError, discord.DiscordException):
                    self.logger.exception(
                        "ticket_script_delivery_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    message_text = (
                        "Payment was confirmed, but I couldn't send the delivery file right "
                        "now. Please contact support."
                    )
                except Exception:
                    self.logger.exception(
                        "payment_success_flow_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                        channel.id,
                        user_id,
                        selected_product.key,
                        utc_timestamp(),
                    )
                    await self.update_ticket_record(
                        channel.id,
                        stage=TICKET_STAGE_PAYMENT_PENDING,
                    )
                    message_text = (
                        "Payment was confirmed, but I couldn't finish the delivery flow right "
                        "now. Please contact support."
                    )
        elif parser_result is not None and parser_result.get("matched") is False:
            await self.update_ticket_record(
                channel.id,
                stage=TICKET_STAGE_AWAITING_PAYMENT,
            )
            message_text = self.build_payment_parser_failure_message(parser_result)

        try:
            await channel.send(
                message_text,
                allowed_mentions=self.response_allowed_mentions,
            )
        except discord.DiscordException:
            self.logger.exception(
                "payment_check_message_send_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                user_id,
                utc_timestamp(),
            )

    def persist_state(self) -> None:
        try:
            save_state(self.state)
        except OSError:
            self.logger.exception("state_save_failed timestamp=%s", utc_timestamp())

    async def get_consumed_payment_message_ids_snapshot(self) -> set[str]:
        async with self.state_lock:
            parser_state = get_payment_parser_state(self.state)
            state_changed = purge_consumed_message_ids(parser_state)
            consumed_message_ids = set(parser_state["consumed_message_ids"])
            if state_changed:
                self.persist_state()
            return consumed_message_ids

    async def record_consumed_payment_message_id(self, gmail_message_id: str) -> bool:
        async with self.state_lock:
            parser_state = get_payment_parser_state(self.state)
            purge_consumed_message_ids(parser_state)
            if gmail_message_id in parser_state["consumed_message_ids"]:
                return False

            record_consumed_message_id(parser_state, gmail_message_id)
            try:
                save_state(self.state)
            except OSError:
                self.logger.exception(
                    "payment_parser_state_save_failed gmail_message_id=%s timestamp=%s",
                    gmail_message_id,
                    utc_timestamp(),
                )
                return False
            return True

    def build_payment_parser_failure_message(
        self,
        parser_result: PaymentParserResult,
    ) -> str:
        reason = cast(str, parser_result.get("reason", "payment not detected yet"))
        if reason == "no candidate messages found":
            return (
                "Payment was not detected in the recent inbox window yet. If you just paid, "
                "wait a moment and press Confirm Payment again. If the payment already went "
                "through, open a support ticket for manual review."
            )
        if reason == "payment note code unavailable":
            return (
                "This ticket is missing its required payment note code. Choose the payment platform again "
                "and use the exact code shown before confirming payment."
            )
        if reason == "payment note missing":
            payment_note = cast(str | None, parser_result.get("expected_payment_note"))
            if payment_note:
                return (
                    f"The receipt email did not contain the required payment code `{payment_note}`. "
                    "Send payment with that exact code in the note or open a support ticket for manual review."
                )
        if reason == "amount short":
            shortfall = cast(str | None, parser_result.get("amount_shortfall"))
            if shortfall:
                return (
                    f"Your payment is ${shortfall} short. Send the remaining ${shortfall} "
                    "or open a support ticket for a refund."
                )
        return (
            f"Automatic payment confirmation did not pass ({reason}). If the payment was "
            "already sent, please open a support ticket for manual review."
        )

    async def send_response(self, message: discord.Message, text: str) -> None:
        chunks = split_message(text)
        first_chunk = True
        for chunk in chunks:
            try:
                if first_chunk:
                    await message.reply(
                        chunk,
                        mention_author=False,
                        allowed_mentions=self.response_allowed_mentions,
                    )
                    first_chunk = False
                else:
                    await message.channel.send(
                        chunk,
                        allowed_mentions=self.response_allowed_mentions,
                    )
            except discord.DiscordException:
                if first_chunk:
                    try:
                        await message.channel.send(
                            chunk,
                            allowed_mentions=self.response_allowed_mentions,
                        )
                        first_chunk = False
                        continue
                    except discord.DiscordException:
                        pass

                self.logger.exception(
                    "message_response_send_failed channel_id=%s user_id=%s timestamp=%s",
                    message.channel.id,
                    message.author.id,
                    utc_timestamp(),
                )
                return

    async def send_script_selection_confirmation(
        self,
        message: discord.Message,
        product: ScriptProduct,
    ) -> bool:
        try:
            await message.reply(
                build_script_confirmation_message(product),
                mention_author=False,
                allowed_mentions=self.response_allowed_mentions,
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "ticket_selection_confirmation_send_failed channel_id=%s user_id=%s script=%s timestamp=%s",
                message.channel.id,
                message.author.id,
                product.key,
                utc_timestamp(),
            )
            await self.send_response(
                message,
                "I couldn't send the confirmation message right now. Please try again shortly.",
            )
            return False

    async def send_payment_platform_prompt(self, message: discord.Message) -> bool:
        channel_id = cast(int, message.channel.id)
        ticket_record = await self.get_ticket_record_snapshot(channel_id)
        selected_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        if selected_product is None:
            await self.send_response(
                message,
                "I couldn't determine which script you selected. Please choose the script again.",
            )
            return False

        try:
            await message.reply(
                build_payment_platform_prompt_message(selected_product),
                mention_author=False,
                view=self.build_payment_platform_selection_view(),
                allowed_mentions=self.response_allowed_mentions,
            )
            return True
        except discord.DiscordException:
            self.logger.exception(
                "ticket_payment_platform_prompt_send_failed channel_id=%s user_id=%s timestamp=%s",
                message.channel.id,
                message.author.id,
                utc_timestamp(),
            )
            await self.send_response(
                message,
                "I couldn't send the payment platform options right now. Please try again shortly.",
            )
            return False

    async def resolve_purchase_user_identity(
        self,
        channel: discord.TextChannel,
        user_id: int,
    ) -> tuple[str, str]:
        member = channel.guild.get_member(user_id)
        if member is not None:
            username = member.name
            display_name = member.display_name or username
            return username, display_name

        cached_user = self.get_user(user_id)
        if cached_user is not None:
            username = cached_user.name
            display_name = getattr(cached_user, "display_name", username) or username
            return username, display_name

        try:
            fetched_user = await self.fetch_user(user_id)
        except discord.DiscordException:
            self.logger.exception(
                "purchase_log_user_lookup_failed channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                user_id,
                utc_timestamp(),
            )
            fallback_name = f"unknown-user-{user_id}"
            return fallback_name, fallback_name

        username = fetched_user.name
        display_name = getattr(fetched_user, "display_name", username) or username
        return username, display_name

    async def build_purchase_record(
        self,
        channel: discord.TextChannel,
        user_id: int,
        product: ScriptProduct,
        *,
        purchase_timestamp: str,
    ) -> PurchaseRecord:
        username, display_name = await self.resolve_purchase_user_identity(channel, user_id)
        guild_id = channel.guild.id if channel.guild is not None else 0
        full_date = datetime.fromisoformat(purchase_timestamp).date().isoformat()
        return {
            "Full Date": full_date,
            "Exact Timestamp": purchase_timestamp,
            "Discord Username": username,
            "Display Name": display_name or username,
            "User ID": user_id,
            "Item Purchased": product.label,
            "Item Key": product.key,
            "Delivered File": product.file_path.name,
            "Price Paid": product.price,
            "Channel ID": channel.id,
            "Guild ID": guild_id,
            "Purchase Event ID": uuid4().hex,
        }

    async def record_successful_purchase(
        self,
        channel: discord.TextChannel,
        user_id: int,
        product: ScriptProduct,
    ) -> None:
        purchase_timestamp = utc_timestamp()
        purchase_record = await self.build_purchase_record(
            channel,
            user_id,
            product,
            purchase_timestamp=purchase_timestamp,
        )
        async with self.purchase_sync_lock:
            try:
                queued_ok, local_ok, sheet_ok = await asyncio.to_thread(
                    self.purchase_logger.queue_and_sync_purchase,
                    purchase_record,
                )
            except Exception:
                self.logger.exception(
                    "purchase_log_record_unexpected_failure channel_id=%s user_id=%s item_key=%s purchase_event_id=%s timestamp=%s",
                    channel.id,
                    user_id,
                    product.key,
                    purchase_record["Purchase Event ID"],
                    purchase_timestamp,
                )
                return

        log_message = (
            "purchase_log_record_processed "
            "channel_id=%s user_id=%s item_key=%s purchase_event_id=%s "
            "timestamp=%s queued_ok=%s local_ok=%s sheet_ok=%s recovery_path=%s"
        )
        if not queued_ok:
            self.logger.error(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )
        elif not (local_ok and sheet_ok):
            self.logger.warning(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )
        else:
            self.logger.info(
                log_message,
                channel.id,
                user_id,
                product.key,
                purchase_record["Purchase Event ID"],
                purchase_timestamp,
                queued_ok,
                local_ok,
                sheet_ok,
                self.purchase_logger.recovery_file,
            )

    async def handle_ticket_prompt(self, message: discord.Message) -> None:
        channel = message.channel
        if not isinstance(channel, discord.TextChannel):
            return

        owner_id = await self.get_authoritative_ticket_owner_id(
            channel,
        )
        if owner_id is None:
            self.logger.warning(
                "ticket_owner_unresolved channel_id=%s user_id=%s timestamp=%s",
                channel.id,
                message.author.id,
                utc_timestamp(),
            )
            await self.send_response(
                message,
                "I couldn't verify the ticket owner for this channel. Please contact support.",
            )
            return
        if owner_id is not None and message.author.id != owner_id:
            self.logger.info(
                "ticket_message_ignored_non_owner channel_id=%s user_id=%s owner_id=%s timestamp=%s",
                channel.id,
                message.author.id,
                owner_id,
                utc_timestamp(),
            )
            return

        ticket_record = await self.get_ticket_record_snapshot(
            channel.id,
        )
        ticket_stage = cast(
            str,
            ticket_record.get("stage", TICKET_STAGE_AWAITING_SELECTION),
        )
        current_product = get_script_product_by_key(
            cast(str | None, ticket_record.get("selected_script_key"))
        )
        current_platform = get_payment_platform_by_key(
            cast(str | None, ticket_record.get("payment_platform_key"))
        )
        current_payment_note_code = cast(str | None, ticket_record.get("payment_note_code"))
        if current_platform is not None and not current_payment_note_code:
            current_payment_note_code = await self.ensure_payment_note_code(
                channel.id,
                owner_id=message.author.id,
            )

        if ticket_stage == TICKET_STAGE_AWAITING_CONFIRMATION:
            if message_is_selection_confirmation(message.content):
                if current_product is None:
                    await self.update_ticket_record(
                        channel.id,
                        owner_id=message.author.id,
                        selected_script_key=None,
                        payment_platform_key=None,
                        payment_note_code=None,
                        stage=TICKET_STAGE_AWAITING_SELECTION,
                    )
                    await self.send_response(message, build_ticket_retry_message())
                    return

                if not await self.send_payment_platform_prompt(message):
                    return

                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_PAYMENT_PLATFORM,
                )
                self.logger.info(
                    "ticket_selection_confirmed channel_id=%s user_id=%s script=%s timestamp=%s",
                    channel.id,
                    message.author.id,
                    current_product.key,
                    utc_timestamp(),
                )
                return

            selected_product = find_script_product(message.content)
            if selected_product is None:
                await self.send_response(
                    message,
                    (
                        f"Type {CONFIRM_SELECTION_RESPONSE} to confirm and proceed, "
                        "or reply with the script's exact name, number, or a clear alias to choose a different script."
                    ),
                )
                return

            await self.update_ticket_record(
                channel.id,
                owner_id=message.author.id,
                selected_script_key=selected_product.key,
                payment_platform_key=None,
                payment_note_code=None,
                stage=TICKET_STAGE_AWAITING_CONFIRMATION,
            )
            self.logger.info(
                "ticket_selection_updated channel_id=%s user_id=%s script=%s timestamp=%s",
                channel.id,
                message.author.id,
                selected_product.key,
                utc_timestamp(),
            )
            await self.send_script_selection_confirmation(message, selected_product)
            return

        if ticket_stage == TICKET_STAGE_AWAITING_PAYMENT_PLATFORM:
            if current_product is None:
                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    selected_script_key=None,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_SELECTION,
                )
                await self.send_response(message, build_ticket_retry_message())
                return

            await self.send_payment_platform_prompt(message)
            return

        if ticket_stage in {
            TICKET_STAGE_AWAITING_PAYMENT,
            TICKET_STAGE_PAYMENT_PENDING,
            TICKET_STAGE_COMPLETED,
        }:
            if current_product is None:
                await self.update_ticket_record(
                    channel.id,
                    owner_id=message.author.id,
                    selected_script_key=None,
                    payment_platform_key=None,
                    payment_note_code=None,
                    stage=TICKET_STAGE_AWAITING_SELECTION,
                )
                await self.send_response(message, build_ticket_retry_message())
                return

            if ticket_stage == TICKET_STAGE_COMPLETED:
                await self.send_response(
                    message,
                    f"Payment has already been confirmed for {current_product.label} in this ticket.",
                )
                return

            if ticket_stage == TICKET_STAGE_PAYMENT_PENDING:
                await self.send_response(
                    message,
                    "A payment check is already running for this ticket.",
                )
                return

            await self.send_response(
                message,
                (
                    f"Your script is confirmed as {current_product.label}. "
                    f"{current_platform.label} is selected. "
                    f"Use the exact payment note code `{current_payment_note_code or 'missing'}` and press Confirm Payment when you're ready."
                )
                if current_platform is not None
                else (
                    f"Your script is confirmed as {current_product.label}. "
                    "Press Confirm Payment when you're ready."
                ),
            )
            return

        selected_product = find_script_product(message.content)
        if selected_product is None:
            self.logger.info(
                "ticket_selection_unmatched channel_id=%s user_id=%s timestamp=%s content=%r",
                channel.id,
                message.author.id,
                utc_timestamp(),
                message.content,
            )
            await self.send_response(message, build_ticket_retry_message())
            return

        await self.update_ticket_record(
            channel.id,
            owner_id=message.author.id,
            selected_script_key=selected_product.key,
            payment_platform_key=None,
            payment_note_code=None,
            stage=TICKET_STAGE_AWAITING_CONFIRMATION,
        )
        self.logger.info(
            "ticket_selection_matched channel_id=%s user_id=%s script=%s timestamp=%s",
            channel.id,
            message.author.id,
            selected_product.key,
            utc_timestamp(),
        )
        await self.send_script_selection_confirmation(message, selected_product)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        if self.is_purchase_ticket_channel(message.channel):
            await self.handle_ticket_prompt(message)
            return
        if self.is_support_ticket_channel(message.channel):
            return

    async def close(self) -> None:
        if self.purchase_sync_retry_task is not None:
            self.purchase_sync_retry_task.cancel()
            await asyncio.gather(
                self.purchase_sync_retry_task,
                return_exceptions=True,
            )
            self.purchase_sync_retry_task = None

        if self.payment_check_tasks:
            pending_tasks = tuple(self.payment_check_tasks)
            for task in pending_tasks:
                task.cancel()
            await asyncio.gather(*pending_tasks, return_exceptions=True)
            self.payment_check_tasks.clear()
            self.pending_payment_check_channel_ids.clear()

        await super().close()
