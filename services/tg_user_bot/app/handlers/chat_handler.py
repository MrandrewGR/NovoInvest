# File location: services/tg_user_bot/app/handlers/chat_handler.py

import logging
import asyncio
from telethon import events
from telethon.errors import FloodWaitError
from ..config import settings
from ..kafka_producer import KafkaMessageProducer
from ..utils import human_like_delay, get_delay_settings
from ..state import MessageCounter

logger = logging.getLogger("handlers.chat_handler")

def register_chat_handler(client, kafka_producer, counter: MessageCounter, userbot_active: asyncio.Event):
    @client.on(events.NewMessage(chats=[settings.TELEGRAM_CHAT_ID]))
    async def handler_chat(event):
        if not userbot_active.is_set():
            logger.info("Userbot приостановлен. Сообщение игнорируется.")
            return
        logger.info(f"В чате новое сообщение")
        await process_message(event, kafka_producer, settings.KAFKA_CHAT_TOPIC, "чата", counter)

async def process_message(event, kafka_producer: KafkaMessageProducer, topic, source, counter: MessageCounter):
    try:
        msg = event.message
        message_data = msg.to_dict()

        reply_data = None
        if msg.reply_to_msg_id:
            reply_msg = await event.get_reply_message()
            if reply_msg:
                reply_data = reply_msg.to_dict()

        downloaded_media_path = None
        if msg.media:
            downloaded_media_path = await event.download_media(file=settings.MEDIA_DIR)

        result_data = {
            "id": msg.id,
            "chat_id": event.chat_id,
            "date": msg.date.isoformat(),
            "original_message": message_data,
            "reply_message": reply_data,
            "downloaded_media": downloaded_media_path
        }

        kafka_producer.send_message(topic, result_data)
        logger.info(f"Сообщение отправлено в Kafka топик '{topic}': {msg.id}")

        await counter.increment()

        delay_min, delay_max = get_delay_settings("chat")
        await human_like_delay(delay_min, delay_max)

    except FloodWaitError as e:
        logger.warning(f"FloodWaitError для {source}: ждать {e.seconds} секунд")
        await asyncio.sleep(e.seconds + 5)
    except Exception as e:
        logger.exception(f"Неожиданная ошибка в обработчике сообщений {source}: {e}")
