# File location: services/tg_user_bot/app/state.py

import asyncio
import logging
from telethon import TelegramClient

logger = logging.getLogger("state")

class MessageCounter:
    def __init__(self, client: TelegramClient, threshold: int = 100):
        self.client = client
        self.threshold = threshold
        self.count = 0
        self.lock = asyncio.Lock()

    async def increment(self):
        async with self.lock:
            self.count += 1
            logger.debug(f"Сообщений обработано: {self.count}")
            if self.count % self.threshold == 0:
                await self.notify()

    async def notify(self):
        try:
            saved_messages = await self.client.get_entity("me")
            notification = f"Обработано {self.count} сообщений."
            await self.client.send_message(saved_messages, notification)
            logger.info(f"Отправлено уведомление о {self.count} обработанных сообщениях.")
        except Exception as e:
            logger.exception(f"Не удалось отправить уведомление о {self.count} обработанных сообщениях: {e}")
