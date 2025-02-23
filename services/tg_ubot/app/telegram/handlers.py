# File: services/tg_ubot/app/telegram/handlers.py

import logging
import asyncio
from telethon import events
from telethon.tl.types import Message

from app.config import settings
from app.utils import human_like_delay, get_delay_settings
from app.process_messages import serialize_message
from mirco_services_data_management.db import ensure_partitioned_parent_table, insert_partitioned_record

logger = logging.getLogger("unified_handler")

def register_unified_handler(
    client,
    message_buffer: asyncio.Queue,
    userbot_active: asyncio.Event,
    chat_id_to_data: dict,
    state_mgr=None
):
    """
    Registers handlers for 'NewMessage' and 'MessageEdited' events
    for the given target_ids (from chat_id_to_data.keys()).
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
    Processes the message event:
      - Executes human-like delay
      - Serializes the message
      - Adds (topic, data) to message_buffer for Kafka
      - Saves the message into the corresponding partitioned table in DB
    """
    try:
        msg: Message = event.message
        chat_info = chat_id_to_data.get(msg.chat_id)
        if not chat_info:
            logger.warning(f"No chat_info for chat_id={msg.chat_id}, skipping.")
            return

        # Human-like delay
        dmin, dmax = get_delay_settings("chat")
        await human_like_delay(dmin, dmax)

        # Serialize
        data = serialize_message(msg, event_type, chat_info)
        if not data:
            return

        # Send to Kafka buffer
        topic = settings.UBOT_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

        # Determine partitioned table name
        if "chat_username" in chat_info and chat_info["chat_username"]:
            table_suffix = chat_info["chat_username"].lstrip("@").lower()
        else:
            table_suffix = str(msg.chat_id)
        table_name = "messages_" + table_suffix

        # Ensure the partitioned parent table (no unique index)
        ensure_partitioned_parent_table(table_name)

        # Deduplicate only new_message; store every edit as a new row
        deduplicate = (event_type == "new_message")

        inserted = insert_partitioned_record(table_name, data, deduplicate=deduplicate)
        if inserted:
            logger.info(
                f"[unified_handler] Message inserted into DB, "
                f"msg_id={msg.id} in table {table_name}"
            )
        else:
            logger.info(
                f"[unified_handler] Duplicate (not inserted), "
                f"msg_id={msg.id} in table {table_name}"
            )

        logger.info(f"[unified_handler] Processed {event_type} msg_id={msg.id} chat_id={msg.chat_id}")
    except Exception as e:
        logger.exception(f"[unified_handler] Error processing message: {e}")
