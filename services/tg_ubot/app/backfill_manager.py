# services/tg_ubot/app/backfill_manager.py

import asyncio
import logging
from telethon import errors
from telethon.tl.patched import Message
from .utils import human_like_delay, get_delay_settings
from .state_manager import StateManager

logger = logging.getLogger("backfill_manager")


class BackfillManager:
    """
    Отвечает за вычитку (backfill) более старых сообщений в тихие моменты,
    чтобы не мешать обработке новых сообщений.
    """

    def __init__(
        self,
        client,
        state_mgr: StateManager,
        message_callback,
        new_msgs_threshold: int = 5,
        idle_timeout: int = 10,
        batch_size: int = 50,
        flood_wait_delay: int = 60,
        max_total_wait: int = 300
    ):
        """
        :param client: Telethon-клиент
        :param state_mgr: менеджер состояния (backfill_from_id, pop_new_messages_count и т.д.)
        :param message_callback: корутина (async def), которая примет dict и отправит в Kafka
        :param new_msgs_threshold: если за idle_timeout секунд пришло больше new_msgs_threshold новых, бэкфилл пропускаем
        :param idle_timeout: раз в сколько секунд проверяем количество новых сообщений
        :param batch_size: сколько старых сообщений подгружаем за один проход
        :param flood_wait_delay: базовая задержка при FloodWaitError
        :param max_total_wait: максимальная задержка при FloodWaitError
        """
        self.client = client
        self.state_mgr = state_mgr
        self.message_callback = message_callback

        self.new_msgs_threshold = new_msgs_threshold
        self.idle_timeout = idle_timeout
        self.batch_size = batch_size
        self.flood_wait_delay = flood_wait_delay
        self.max_total_wait = max_total_wait

        self._stop_event = asyncio.Event()

    def stop(self):
        """Вызывается при завершении, чтобы остановить цикл бэкфилла."""
        self._stop_event.set()

    async def run(self):
        """Основной цикл: каждые idle_timeout секунд проверяем поток новых сообщений и, если тихо, подгружаем старые."""
        logger.info("BackfillManager запущен.")
        while not self._stop_event.is_set():
            # Ждём idle_timeout
            await asyncio.sleep(self.idle_timeout)

            # Смотрим, сколько новых сообщений за эти idle_timeout секунд
            new_count = self.state_mgr.pop_new_messages_count(interval=self.idle_timeout)
            logger.debug(f"[Backfill] За {self.idle_timeout}сек пришло {new_count} новых сообщений.")

            if new_count > self.new_msgs_threshold:
                logger.debug("[Backfill] Поступает много новых сообщений, откладываем бэкфилл.")
                continue

            # Подгружаем старые (если есть)
            chats_to_backfill = self.state_mgr.get_chats_needing_backfill()
            if not chats_to_backfill:
                logger.debug("[Backfill] Нет чатов, требующих бэкфилла.")
                continue

            for chat_id in chats_to_backfill:
                if self._stop_event.is_set():
                    break
                await self._do_chat_backfill(chat_id)

        logger.info("BackfillManager остановлен.")

    async def _do_chat_backfill(self, chat_id: int):
        """
        Подгрузка batch_size старых сообщений для одного чата, начиная с backfill_from_id.
        """
        backfill_from_id = self.state_mgr.get_backfill_from_id(chat_id)
        if backfill_from_id is None or backfill_from_id <= 1:
            logger.debug(f"[Backfill] Чат {chat_id} уже полностью выгружен.")
            return

        logger.info(
            f"[Backfill] Загружаем до {self.batch_size} старых сообщений для чата {chat_id}, offset_id={backfill_from_id}"
        )
        try:
            messages = await self.client.get_messages(
                entity=chat_id,
                limit=self.batch_size,
                offset_id=backfill_from_id,
                reverse=False
            )
            if not messages:
                logger.info(f"[Backfill] В чате {chat_id} нет более старых сообщений.")
                return

            # Сортируем убыванию ID
            sorted_msgs = sorted(messages, key=lambda m: m.id, reverse=True)

            for msg in sorted_msgs:
                if self._stop_event.is_set():
                    break

                if msg.id >= backfill_from_id:
                    # Это не "старое" сообщение
                    continue

                # Задержка (как в unified_handler)
                dmin, dmax = get_delay_settings("chat")
                await human_like_delay(dmin, dmax)

                # Отправляем в Kafka
                data = await self._serialize_message(msg)
                await self.message_callback(data)

                # Смещаем backfill_from_id
                if msg.id < backfill_from_id:
                    backfill_from_id = msg.id

            # Сохраняем новое значение backfill_from_id
            self.state_mgr.update_backfill_from_id(chat_id, backfill_from_id)
            logger.info(f"[Backfill] Новый backfill_from_id для {chat_id} = {backfill_from_id}")

        except asyncio.CancelledError:
            raise
        except errors.FloodWaitError as e:
            wait_sec = min(e.seconds + self.flood_wait_delay, self.max_total_wait)
            logger.warning(
                f"[Backfill] FloodWaitError: Telegram просит подождать {e.seconds}сек. "
                f"Ждем {wait_sec}сек, чтобы не превышать лимиты."
            )
            await asyncio.sleep(wait_sec)
        except Exception as e:
            logger.exception(f"[Backfill] Ошибка при бэкфилле чата {chat_id}: {e}")

    async def _serialize_message(self, msg: Message) -> dict:
        """
        Упрощённая сериализация "старого" сообщения в JSON, аналогичная logic unified_handler.
        """
        result = {
            "event_type": "backfill_message",
            "message_id": msg.id,
            "chat_id": msg.chat_id,
            "date": msg.date.isoformat() if msg.date else None,
            "text_plain": msg.message or "",
            "month_part": msg.date.strftime('%Y-%m') if msg.date else None,
            "sender_id": msg.sender_id,
        }
        return result