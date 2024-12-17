# app/handlers/channel_handler.py

import logging
from telethon import events
from telethon.errors import FloodWaitError
from ..config import settings
from ..kafka_producer import KafkaMessageProducer
from ..utils import human_like_delay, get_delay_settings
from ..state import MessageCounter

logger = logging.getLogger("handlers.channel_handler")

def register_channel_handler(client, kafka_producer, counter: MessageCounter):
    @client.on(events.NewMessage(chats=[settings.TELEGRAM_CHANNEL_ID]))
    async def handler_channel(event):
        await process_message(event, kafka_producer, settings.KAFKA_CHANNEL_TOPIC, "канала", counter)

async def process_message(event, kafka_producer, topic, source, counter: MessageCounter):
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

        # Увеличиваем счётчик сообщений
        await counter.increment()

        # Если есть необходимость в задержке после обработки сообщения
        delay_min, delay_max = get_delay_settings("channel")
        await human_like_delay(delay_min, delay_max)

    except FloodWaitError as e:
        logger.warning(f"FloodWaitError для {source}: ждать {e.seconds} секунд")
        await asyncio.sleep(e.seconds + 5)  # Добавляем дополнительную задержку
    except Exception as e:
        logger.exception(f"Неожиданная ошибка в обработчике сообщений {source}: {e}")
