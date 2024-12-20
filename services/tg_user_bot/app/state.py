# File location: services/tg_user_bot/app/state.py

import asyncio
import logging
from telethon import TelegramClient
import json
import os

logger = logging.getLogger("state")

class MessageCounter:
    def __init__(self, client: TelegramClient, threshold: int = 100):
        self.client = client
        self.threshold = threshold
        self.count = 0
        self.lock = asyncio.Lock()
        self.state_file = "/app/state.json"
        self.state = self.load_state()

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.debug("Состояние успешно загружено.")
                return state
            except Exception as e:
                logger.exception(f"Не удалось загрузить состояние: {e}")
        return {}

    def save_state(self):
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f)
            logger.debug("Состояние успешно сохранено.")
        except Exception as e:
            logger.exception(f"Не удалось сохранить состояние: {e}")

    def get_last_message_id(self, chat_id):
        return self.state.get(str(chat_id))

    def update_last_message_id(self, chat_id, message_id):
        self.state[str(chat_id)] = message_id
        self.save_state()

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
