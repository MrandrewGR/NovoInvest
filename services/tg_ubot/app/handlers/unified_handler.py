# services/tg_ubot/app/handlers/unified_handler.py

import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import Message

from app.config import settings
from app.process_messages import serialize_message
from app.utils import human_like_delay, get_delay_settings
import json

logger = logging.getLogger("unified_handler")


def register_unified_handler(
    client: TelegramClient,
    message_buffer: asyncio.Queue,
    counter,
    userbot_active: asyncio.Event,
    chat_id_to_data: dict,
    state_mgr=None
):
    """
    Registers a unified handler for all "new" and "edited" messages
    in the list of target chats/channels.

    Parameters:
    - client: Telethon client
    - message_buffer: asyncio.Queue to send (topic, data) for Kafka
    - counter: MessageCounter instance
    - userbot_active: asyncio.Event to toggle userbot activity
    - chat_id_to_data: Dictionary of chat metadata (result of get_all_chats_info)
    - state_mgr (optional): If provided, calls state_mgr.record_new_message() on each new/edited message.
    """
    target_ids = settings.TELEGRAM_TARGET_IDS
    logger.info(f"Registering unified_handler for chats/channels: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        # Record new message in state_mgr for backfill purposes
        if state_mgr is not None:
            state_mgr.record_new_message()

        # Check if userbot is active
        if not userbot_active.is_set():
            return

        await process_message_event(
            event=event,
            event_type="new_message",
            message_buffer=message_buffer,
            counter=counter,
            chat_id_to_data=chat_id_to_data
        )

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        # Record edited message in state_mgr for backfill purposes
        if state_mgr is not None:
            state_mgr.record_new_message()

        # Check if userbot is active
        if not userbot_active.is_set():
            return

        await process_message_event(
            event=event,
            event_type="edited_message",
            message_buffer=message_buffer,
            counter=counter,
            chat_id_to_data=chat_id_to_data
        )


async def process_message_event(event, event_type, message_buffer, counter, chat_id_to_data):
    """
    Processes a single incoming or edited message:
      1. Human-like delay (day/night consideration)
      2. Collect data, including possible media, reactions, etc.
      3. Serialize the message into JSON
      4. Send to Kafka via message_buffer

    Parameters:
    - event: Telethon event
    - event_type: "new_message" or "edited_message"
    - message_buffer: asyncio.Queue for Kafka
    - counter: MessageCounter instance
    - chat_id_to_data: Dictionary of chat metadata
    """
    try:
        msg: Message = event.message
        logger.debug(f"[unified_handler] Received message: {msg.id} from chat_id={msg.chat_id}")

        # Check if chat_id is described in chat_id_to_data
        logger.debug(f"Available chat_ids in chat_id_to_data: {list(chat_id_to_data.keys())}")
        chat_id = msg.chat_id
        chat_info = chat_id_to_data.get(chat_id)
        if not chat_info:
            logger.warning(f"[unified_handler] chat_id={chat_id} not found in chat_id_to_data. Skipping.")
            return

        # Delay based on day/night settings
        min_delay, max_delay = get_delay_settings("chat")
        await human_like_delay(min_delay, max_delay)

        # Increment processed messages counter
        await counter.increment()

        # Serialize the message
        message_data = serialize_message(msg, event_type=event_type, chat_info=chat_info)

        # Log the serialized message
        try:
            message_json = json.dumps(message_data, ensure_ascii=False)
            logger.info(f"[unified_handler] Final JSON for Kafka: {message_json}")
        except (TypeError, ValueError) as e:
            logger.error(f"[unified_handler] Error serializing message_data to JSON: {e}")

        # Send to Kafka
        kafka_topic = settings.KAFKA_UBOT_OUTPUT_TOPIC
        await message_buffer.put((kafka_topic, message_data))
        logger.info(f"[unified_handler] Processed message {msg.id} from {chat_info.get('chat_title')}, name_uname={chat_info.get('name_uname')}")

    except Exception as e:
        logger.exception(f"[unified_handler] Error in process_message_event: {e}")
