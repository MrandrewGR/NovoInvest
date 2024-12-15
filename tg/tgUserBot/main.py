import asyncio
import logging
import signal
import os
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from config import Config
from kafka_producer import KafkaMessageProducer
from utils import human_like_delay, ensure_dir

# Настройка логирования
log_dir = os.path.dirname(Config.LOG_FILE)
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=Config.LOG_FILE,
    format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
    level=getattr(logging, Config.LOG_LEVEL),
)

# Создание необходимых директорий
ensure_dir(Config.MEDIA_DIR)

# Инициализация Kafka продюсера
kafka_producer = KafkaMessageProducer(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)

# Инициализация клиента с использованием файла сессии
session_file = 'session_name'
client = TelegramClient(session_file, Config.API_ID, Config.API_HASH)
shutdown_event = asyncio.Event()

# Обработчики сообщений
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

        kafka_producer.send_message(Config.KAFKA_CHANNEL_TOPIC, result_data)
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError for channel: wait %s seconds", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception:
        logger.exception("Unexpected error in channel message handler.")

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

        kafka_producer.send_message(Config.KAFKA_CHAT_TOPIC, result_data)
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError for chat: wait %s seconds", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception:
        logger.exception("Unexpected error in chat message handler.")

# Запуск бота
async def run_userbot():
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Telegram client is not authorized. Ensure the session file is correct.")
        shutdown_event.set()
        return
    me = await client.get_me()
    logger.info("Userbot started as: %s (ID: %s)", me.username, me.id)

    # Ожидание завершения
    done, pending = await asyncio.wait(
        [client.run_until_disconnected(), shutdown_event.wait()],
        return_when=asyncio.FIRST_COMPLETED
    )

    if shutdown_event.is_set():
        logger.info("Shutting down Telegram client...")
        await client.disconnect()

# Обработчик сигнала завершения
def shutdown_signal_handler(signum, frame):
    logger.info("Received shutdown signal (%s), initiating graceful shutdown...", signum)
    shutdown_event.set()

# Основная функция
async def main():
    loop = asyncio.get_event_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(s, shutdown_signal_handler, s, None)
    await run_userbot()

# Запуск
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Userbot stopped by user request.")
    finally:
        kafka_producer.close()
        logger.info("Userbot service stopped gracefully.")
