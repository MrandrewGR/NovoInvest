import asyncio
import logging
import signal
import os
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from userbot.config import Config
from userbot.kafka_producer import KafkaMessageProducer
from userbot.utils import human_like_delay, ensure_dir

logger = logging.getLogger(__name__)

logging.basicConfig(
    filename=Config.LOG_FILE,
    format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
    level=getattr(logging, Config.LOG_LEVEL),
)

ensure_dir(Config.MEDIA_DIR)
ensure_dir(os.path.dirname(Config.LOG_FILE))

kafka_producer = KafkaMessageProducer(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
client = TelegramClient('userbot_session', Config.API_ID, Config.API_HASH)
shutdown_event = asyncio.Event()


# Обработчик для канала
@client.on(events.NewMessage(chats=[Config.CHANNEL_ID]))
async def handler_channel(event):
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
            downloaded_media_path = await client.download_media(msg, Config.MEDIA_DIR)

        result_data = {
            "id": msg.id,
            "chat_id": event.chat_id,
            "date": msg.date.isoformat(),
            "original_message": message_data,
            "reply_message": reply_data,
            "downloaded_media": downloaded_media_path
        }

        # Отправляем в топик для канала
        kafka_producer.send_message(Config.KAFKA_CHANNEL_TOPIC, result_data)
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError for channel: wait %s seconds", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception:
        logger.exception("Unexpected error in channel message handler.")


# Обработчик для чата
@client.on(events.NewMessage(chats=[Config.CHAT_ID]))
async def handler_chat(event):
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
            downloaded_media_path = await client.download_media(msg, Config.MEDIA_DIR)

        result_data = {
            "id": msg.id,
            "chat_id": event.chat_id,
            "date": msg.date.isoformat(),
            "original_message": message_data,
            "reply_message": reply_data,
            "downloaded_media": downloaded_media_path
        }

        # Отправляем в топик для чата
        kafka_producer.send_message(Config.KAFKA_CHAT_TOPIC, result_data)
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError for chat: wait %s seconds", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception:
        logger.exception("Unexpected error in chat message handler.")


async def run_userbot():
    await client.start(phone=Config.PHONE)
    me = await client.get_me()
    logger.info("Userbot started as: %s (ID: %s)", me.username, me.id)

    await human_like_delay(1, 3)

    done, pending = await asyncio.wait(
        [client.run_until_disconnected(), shutdown_event.wait()],
        return_when=asyncio.FIRST_COMPLETED
    )

    if shutdown_event.is_set():
        logger.info("Shutdown event triggered. Disconnecting Telegram client...")
        await client.disconnect()


def shutdown_signal_handler(signum, frame):
    logger.info("Received shutdown signal (%s), initiating graceful shutdown...", signum)
    shutdown_event.set()


def main():
    loop = asyncio.get_event_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(s, shutdown_signal_handler, s, None)

    try:
        loop.run_until_complete(run_userbot())
    except KeyboardInterrupt:
        logger.info("Userbot stopped by user request.")
    finally:
        kafka_producer.close()
        loop.close()
        logger.info("Userbot service stopped gracefully.")


if __name__ == "__main__":
    main()
