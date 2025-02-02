# services/tg_ubot/app/telegram/backfill.py

import asyncio
import logging
from telethon import errors

from app.utils import human_like_delay, get_delay_settings
from .state_manager import StateManager
from app.process_messages import serialize_message

logger = logging.getLogger("backfill_manager")


class BackfillManager:
    """
    Отвечает за дозагрузку старых сообщений (backfill),
    когда нет большого потока новых.
    """

    def __init__(
        self,
        client,
        state_mgr: StateManager,
        message_callback,
        chat_id_to_data,
        new_msgs_threshold=5,
        idle_timeout=10,
        batch_size=50,
        flood_wait_delay=60,
        max_total_wait=300
    ):
        self.client = client
        self.state_mgr = state_mgr
        self.message_callback = message_callback  # функция, которая сохранит/отправит сообщение
        self.chat_id_to_data = chat_id_to_data

        self.new_msgs_threshold = new_msgs_threshold
        self.idle_timeout = idle_timeout
        self.batch_size = batch_size
        self.flood_wait_delay = flood_wait_delay
        self.max_total_wait = max_total_wait

        self._stop_event = asyncio.Event()

    def stop(self):
        """Вызывается при завершении, чтобы остановить цикл backfill."""
        self._stop_event.set()

    async def run(self):
        """Основной цикл backfill, каждые idle_timeout сек проверяем нагрузку и, если можно, подгружаем старые сообщения."""
        logger.info("BackfillManager started.")
        while not self._stop_event.is_set():
            await asyncio.sleep(self.idle_timeout)

            new_count = self.state_mgr.pop_new_messages_count(self.idle_timeout)
            logger.debug(f"[Backfill] {new_count} new msgs in last {self.idle_timeout} sec.")

            if new_count > self.new_msgs_threshold:
                logger.debug("[Backfill] Too many new msgs => skip backfill.")
                continue

            # Смотрим чаты, у которых backfill_from_id>1
            chats_to_backfill = self.state_mgr.get_chats_needing_backfill()
            if not chats_to_backfill:
                logger.debug("[Backfill] No chats need backfill.")
                continue

            for cid in chats_to_backfill:
                if self._stop_event.is_set():
                    break
                await self._do_chat_backfill(cid)

        logger.info("BackfillManager stopped.")

    async def _do_chat_backfill(self, chat_id: int):
        """
        Собираем последние 'batch_size' старых сообщений (offset_id=backfill_from_id),
        сериализуем, сохраняем, обновляем backfill_from_id.
        """
        backfill_from_id = self.state_mgr.get_backfill_from_id(chat_id)
        if not backfill_from_id or backfill_from_id <= 1:
            logger.debug(f"[Backfill] Chat {chat_id} fully backfilled.")
            return

        logger.info(f"[Backfill] Loading up to {self.batch_size} older msgs for chat {chat_id}, offset={backfill_from_id}")

        try:
            messages = await self.client.get_messages(
                entity=chat_id,
                limit=self.batch_size,
                offset_id=backfill_from_id,
                reverse=False
            )
            if not messages:
                logger.info(f"[Backfill] No older messages in chat {chat_id}, set backfill_from_id=1.")
                self.state_mgr.update_backfill_from_id(chat_id, 1)
                return

            sorted_msgs = sorted(messages, key=lambda m: m.id, reverse=True)

            for msg in sorted_msgs:
                if self._stop_event.is_set():
                    break
                if msg.id >= backfill_from_id:
                    continue

                dmin, dmax = get_delay_settings("chat")
                await human_like_delay(dmin, dmax)

                chat_info = self.chat_id_to_data.get(chat_id, {})
                data = serialize_message(msg, "backfill_message", chat_info)
                await self.message_callback(data)

                if msg.id < backfill_from_id:
                    backfill_from_id = msg.id

            self.state_mgr.update_backfill_from_id(chat_id, backfill_from_id)
            logger.info(f"[Backfill] Updated backfill_from_id for {chat_id} => {backfill_from_id}")

        except asyncio.CancelledError:
            raise
        except errors.FloodWaitError as e:
            wait_sec = min(e.seconds + self.flood_wait_delay, self.max_total_wait)
            logger.warning(f"[Backfill] FloodWaitError => wait {wait_sec}s.")
            await asyncio.sleep(wait_sec)
        except Exception as e:
            logger.exception(f"[Backfill] Error in chat {chat_id}: {e}")
