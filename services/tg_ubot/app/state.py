# File location: services/tg_ubot/app/state.py

import asyncio
import logging
from telethon import TelegramClient
import json
import os

logger = logging.getLogger("state")

class MessageCounter:
    """
    Simple counter that increments whenever a message is processed.
    Sends a Telegram notification every `threshold` messages.
    Persists its count in a local state file between runs.
    """

    def __init__(self, client: TelegramClient, threshold: int = 100):
        self.client = client
        self.threshold = threshold
        self.count = 0
        self.lock = asyncio.Lock()
        self.state_file = "/app/state.json"
        self.state = self.load_state()

        # If we previously had a message_count stored, restore it
        if "message_count" in self.state:
            self.count = self.state["message_count"]

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                logger.debug("MessageCounter state loaded successfully.")
                return state
            except Exception as e:
                logger.exception(f"Failed to load MessageCounter state: {e}")
        return {}

    def save_state(self):
        try:
            self.state["message_count"] = self.count
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)
            logger.debug("MessageCounter state saved successfully.")
        except Exception as e:
            logger.exception(f"Failed to save MessageCounter state: {e}")

    async def increment(self):
        async with self.lock:
            self.count += 1
            logger.debug(f"Processed messages count: {self.count}")
            self.save_state()

            if self.count % self.threshold == 0:
                await self.notify()

    async def notify(self):
        """
        Every time the threshold is reached, send a note to 'Saved Messages'.
        """
        try:
            saved_messages = await self.client.get_entity("me")
            notification = f"Processed {self.count} messages so far."
            await self.client.send_message(saved_messages, notification)
            logger.info(f"Notification sent for {self.count} processed messages.")
        except Exception as e:
            logger.exception(f"Failed to send notification for {self.count} messages: {e}")
