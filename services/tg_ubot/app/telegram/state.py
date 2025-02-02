# services/tg_ubot/app/telegram/state.py

import asyncio
import logging
import json
import os
from telethon import TelegramClient

logger = logging.getLogger("state")

class MessageCounter:
    """
    Простой счётчик обработанных сообщений,
    отправляет уведомление каждые threshold штук в 'Saved Messages'.
    """

    def __init__(self, client: TelegramClient, threshold: int = 100):
        self.client = client
        self.threshold = threshold
        self.count = 0
        self.lock = asyncio.Lock()
        self.state_file = "/app/data/state.json"
        self.state = self.load_state()

        if "message_count" in self.state:
            self.count = self.state["message_count"]

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    s = json.load(f)
                logger.debug("MessageCounter state loaded.")
                return s
            except Exception as e:
                logger.exception(f"Failed to load MessageCounter state: {e}")
        return {}

    def save_state(self):
        try:
            self.state["message_count"] = self.count
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)
            logger.debug("MessageCounter state saved.")
        except Exception as e:
            logger.exception(f"Failed to save MessageCounter state: {e}")

    async def increment(self):
        """
        Увеличиваем счётчик, сохраняем state, при threshold — уведомляем в Telegram.
        """
        async with self.lock:
            self.count += 1
            logger.debug(f"Processed messages: {self.count}")
            self.save_state()

            if self.count % self.threshold == 0:
                await self.notify()

    async def notify(self):
        """
        Шлёт служебное сообщение себе (Saved Messages).
        """
        try:
            saved = await self.client.get_entity("me")
            text = f"Processed {self.count} messages so far."
            await self.client.send_message(saved, text)
            logger.info(f"Notification sent for {self.count} messages.")
        except Exception as e:
            logger.exception(f"Failed to notify for {self.count} messages: {e}")
