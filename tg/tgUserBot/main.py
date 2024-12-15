import asyncio
import logging
import signal
import os
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from config import Config
from kafka_producer import KafkaMessageProducer
from utils import human_like_delay, ensure_dir

# Установка значений по умолчанию для переменных окружения
LOG_FILE = Config.LOG_FILE if Config.LOG_FILE else "logs/userbot.log"
LOG_LEVEL = Config.LOG_LEVEL if Config.LOG_LEVEL else "INFO"
MEDIA_DIR = Config.MEDIA_DIR if Config.MEDIA_DIR else "media"

# Определение директории для логов
log_dir = os.path.dirname(LOG_FILE) if os.path.dirname(LOG_FILE) else "logs"

# Создание директории для логов, если она не существует
if not os.path.exists(log_dir):
    try:
        os.makedirs(log_dir, exist_ok=True)
        print(f"Создана директория для логов: {log_dir}")
    except Exception as e:
        print(f"Не удалось создать директорию для логов: {log_dir}. Ошибка: {e}")
        exit(1)

# Настройка логирования
logging.basicConfig(
    filename=LOG_FILE,
    format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
)
logger = logging.getLogger(__name__)

# Создание необходимых директорий для медиа, если они не существуют
ensure_dir(MEDIA_DIR)

# Инициализация Kafka продюсера
try:
    kafka_producer = KafkaMessageProducer(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Kafka producer успешно инициализирован.")
except Exception as e:
    logger.exception("Не удалось инициализировать Kafka producer: %s", e)
    exit(1)

# Инициализация клиента с использованием файла сессии
session_file = 'session_name'
try:
    client = TelegramClient(session_file, Config.API_ID, Config.API_HASH)
    shutdown_event = asyncio.Event()
except Exception as e:
    logger.exception("Не удалось инициализировать TelegramClient: %s", e)
    exit(1)

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
            downloaded_media_path = await client.download_media(msg, MEDIA_DIR)

        result_data = {
            "id": msg.id,
            "chat_id": event.chat_id,
            "date": msg.date.isoformat(),
            "original_message": message_data,
            "reply_message": reply_data,
            "downloaded_media": downloaded_media_path
        }

        kafka_producer.send_message(Config.KAFKA_CHANNEL_TOPIC, result_data)
        logger.info(f"Сообщение отправлено в Kafka топик '{Config.KAFKA_CHANNEL_TOPIC}': {msg.id}")
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError для канала: ждать %s секунд", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.exception("Неожиданная ошибка в обработчике сообщений канала: %s", e)

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
            downloaded_media_path = await client.download_media(msg, MEDIA_DIR)

        result_data = {
            "id": msg.id,
            "chat_id": event.chat_id,
            "date": msg.date.isoformat(),
            "original_message": message_data,
            "reply_message": reply_data,
            "downloaded_media": downloaded_media_path
        }

        kafka_producer.send_message(Config.KAFKA_CHAT_TOPIC, result_data)
        logger.info(f"Сообщение отправлено в Kafka топик '{Config.KAFKA_CHAT_TOPIC}': {msg.id}")
        await human_like_delay()

    except FloodWaitError as e:
        logger.warning("FloodWaitError для чата: ждать %s секунд", e.seconds)
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.exception("Неожиданная ошибка в обработчике сообщений чата: %s", e)

# Запуск бота
async def run_userbot():
    try:
        await client.connect()
        logger.info("Подключение к Telegram клиенту...")

        if not await client.is_user_authorized():
            logger.error("Telegram клиент не авторизован. Проверьте корректность файла сессии.")
            shutdown_event.set()
            return

        me = await client.get_me()
        logger.info("Userbot запущен как: @%s (ID: %s)", me.username, me.id)

        # Ожидание завершения
        done, pending = await asyncio.wait(
            [client.run_until_disconnected(), shutdown_event.wait()],
            return_when=asyncio.FIRST_COMPLETED
        )

        if shutdown_event.is_set():
            logger.info("Инициирование завершения работы Telegram клиента...")
            await client.disconnect()

    except Exception as e:
        logger.exception("Ошибка при запуске userbot: %s", e)
        shutdown_event.set()

# Обработчик сигнала завершения
def shutdown_signal_handler(signum, frame):
    logger.info("Получен сигнал завершения (%s), инициирование плавного завершения...", signum)
    shutdown_event.set()

# Основная функция
async def main():
    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(s, shutdown_signal_handler, s, None)
    await run_userbot()

# Запуск
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Userbot остановлен по запросу пользователя.")
    except Exception as e:
        logger.exception("Неожиданная ошибка при запуске main: %s", e)
    finally:
        try:
            kafka_producer.close()
            logger.info("Kafka producer закрыт.")
        except Exception as e:
            logger.exception("Ошибка при закрытии Kafka producer: %s", e)
        logger.info("Сервис Userbot завершил работу корректно.")
