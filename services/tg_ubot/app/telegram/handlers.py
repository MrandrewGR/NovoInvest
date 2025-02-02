# services/tg_ubot/app/telegram/handlers.py

import logging
import asyncio

from telethon import events
from telethon.tl.types import Message

from app.config import settings
from app.utils import human_like_delay, get_delay_settings
from app.process_messages import serialize_message

logger = logging.getLogger("unified_handler")

def register_unified_handler(
    client,
    message_buffer: asyncio.Queue,
    userbot_active: asyncio.Event,
    chat_id_to_data: dict,
    state_mgr=None
):
    """
    Регистрируем хендлеры на 'NewMessage' и 'MessageEdited'
    для заданных target_ids (из chat_id_to_data.keys()).
    """

    target_ids = list(chat_id_to_data.keys())
    logger.info(f"Registering unified_handler for chats: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        if state_mgr is not None:
            state_mgr.record_new_message()
        if not userbot_active.is_set():
            return
        await process_message_event(event, "new_message", message_buffer, chat_id_to_data)

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        if state_mgr is not None:
            state_mgr.record_new_message()
        if not userbot_active.is_set():
            return
        await process_message_event(event, "edited_message", message_buffer, chat_id_to_data)


async def process_message_event(event, event_type, message_buffer, chat_id_to_data):
    """
    Обрабатываем сообщение:
     - задержка (day/night)
     - сериализация
     - добавление (topic, data) в message_buffer -> будет отправлено в Kafka
    """
    try:
        msg: Message = event.message
        chat_info = chat_id_to_data.get(msg.chat_id)
        if not chat_info:
            logger.warning(f"No chat_info for chat_id={msg.chat_id}, skipping.")
            return

        # Имитация user delay
        dmin, dmax = get_delay_settings("chat")
        await human_like_delay(dmin, dmax)

        # Сериализация
        data = serialize_message(msg, event_type, chat_info)
        if not data:
            return

        # Кладём в очередь, дальше producer_task отправит в Kafka
        topic = settings.KAFKA_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

        logger.info(f"[unified_handler] Processed {event_type} msg_id={msg.id} chat_id={msg.chat_id}")
    except Exception as e:
        logger.exception(f"[unified_handler] Error processing message: {e}")
