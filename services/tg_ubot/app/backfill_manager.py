# services/tg_ubot/app/backfill_manager.py

import asyncio
import logging
from telethon import errors

from .utils import human_like_delay, get_delay_settings
from .state_manager import StateManager
from .process_messages import get_table_name, serialize_message

logger = logging.getLogger("backfill_manager")


class BackfillManager:
    """
    Responsible for backfilling older messages during idle periods
    to avoid interfering with the processing of new messages.

    The logic:
    - Every `idle_timeout` seconds, check how many new messages came in
      (via state_mgr.pop_new_messages_count).
    - If new messages exceed `new_msgs_threshold`, we skip backfill
      to prioritize new messages.
    - If below threshold, we proceed to backfill in batches.
    """

    def __init__(
        self,
        client,
        state_mgr: StateManager,
        message_callback,
        chat_id_to_data,
        new_msgs_threshold: int = 5,
        idle_timeout: int = 10,
        batch_size: int = 50,
        flood_wait_delay: int = 60,
        max_total_wait: int = 300
    ):
        self.client = client
        self.state_mgr = state_mgr
        self.message_callback = message_callback
        self.chat_id_to_data = chat_id_to_data

        self.new_msgs_threshold = new_msgs_threshold
        self.idle_timeout = idle_timeout
        self.batch_size = batch_size
        self.flood_wait_delay = flood_wait_delay
        self.max_total_wait = max_total_wait

        self._stop_event = asyncio.Event()

    def stop(self):
        """Called on shutdown to stop the backfill loop."""
        self._stop_event.set()

    async def run(self):
        logger.info("BackfillManager started.")
        while not self._stop_event.is_set():
            # Wait for the idle period
            await asyncio.sleep(self.idle_timeout)

            new_count = self.state_mgr.pop_new_messages_count(interval=self.idle_timeout)
            logger.debug(f"[Backfill] {new_count} new messages received in the last {self.idle_timeout} seconds.")

            # If we exceeded threshold, skip backfill for this cycle
            if new_count > self.new_msgs_threshold:
                logger.debug("[Backfill] High volume of new messages, delaying backfill.")
                continue

            chats_to_backfill = self.state_mgr.get_chats_needing_backfill()
            if not chats_to_backfill:
                logger.debug("[Backfill] No chats require backfilling.")
                continue

            for chat_id in chats_to_backfill:
                if self._stop_event.is_set():
                    break
                await self._do_chat_backfill(chat_id)

        logger.info("BackfillManager stopped.")

    async def _do_chat_backfill(self, chat_id: int):
        backfill_from_id = self.state_mgr.get_backfill_from_id(chat_id)
        if backfill_from_id is None or backfill_from_id <= 1:
            logger.debug(f"[Backfill] Chat {chat_id} is fully backfilled (or no older msgs).")
            return

        # Get name_uname for table naming
        chat_info = self.chat_id_to_data.get(chat_id, {})
        name_uname = chat_info.get("name_uname", "Unknown")
        table_name = get_table_name(name_uname, chat_id)

        logger.info(
            f"[Backfill] Loading up to {self.batch_size} older messages for chat {chat_id}, offset_id={backfill_from_id}"
        )
        try:
            messages = await self.client.get_messages(
                entity=chat_id,
                limit=self.batch_size,
                offset_id=backfill_from_id,
                reverse=False
            )
            if not messages:
                logger.info(f"[Backfill] No older messages found in chat {chat_id}.")
                self.state_mgr.update_backfill_from_id(chat_id, 1)
                return

            sorted_msgs = sorted(messages, key=lambda m: m.id, reverse=True)

            for msg in sorted_msgs:
                if self._stop_event.is_set():
                    break

                # Only process messages with ID < backfill_from_id
                if msg.id >= backfill_from_id:
                    continue

                dmin, dmax = get_delay_settings("chat")
                await human_like_delay(dmin, dmax)

                message_data = serialize_message(msg, event_type="backfill_message", chat_info=chat_info)
                await self.message_callback(message_data)

                if msg.id < backfill_from_id:
                    backfill_from_id = msg.id

            self.state_mgr.update_backfill_from_id(chat_id, backfill_from_id)
            logger.info(f"[Backfill] Updated backfill_from_id for {chat_id} to {backfill_from_id}")

        except asyncio.CancelledError:
            raise
        except errors.FloodWaitError as e:
            wait_sec = min(e.seconds + self.flood_wait_delay, self.max_total_wait)
            logger.warning(
                f"[Backfill] FloodWaitError: Telegram requires waiting for {e.seconds} seconds. "
                f"Waiting for {wait_sec} seconds to avoid rate limits."
            )
            await asyncio.sleep(wait_sec)
        except Exception as e:
            logger.exception(f"[Backfill] Error during backfilling chat {chat_id}: {e}")
