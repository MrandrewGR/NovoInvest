# app/main.py

import asyncio
import logging
import os
import signal

from telethon import TelegramClient

from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .handlers import register_handlers
from .utils import ensure_dir
from .state import MessageCounter

async def run_userbot():
    # Создание необходимых директорий
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))  # Убедитесь, что директория для логов существует

    # Настройка логирования
    logger = setup_logging()

    # Инициализация Telegram клиента
    client = TelegramClient('session_name', settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    # Инициализация Kafka продюсера
    try:
        kafka_producer = KafkaMessageProducer()
    except Exception:
        logger.critical("Не удалось инициализировать Kafka producer. Завершение работы.")
        return

    # Инициализация счётчика сообщений
    counter = MessageCounter(client)

    # Регистрация обработчиков
    register_handlers(client, kafka_producer, counter)

    shutdown_event = asyncio.Event()

    # Обработчик сигнала завершения
    def shutdown_signal_handler(signum, frame):
        logger.info(f"Получен сигнал завершения ({signum}), инициирование плавного завершения...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(s, shutdown_signal_handler, s, None)

    try:
        await client.start()
        logger.info("Подключение к Telegram клиенту...")

        if not await client.is_user_authorized():
            logger.error("Telegram клиент не авторизован. Проверьте корректность файла сессии.")
            shutdown_event.set()

        me = await client.get_me()
        logger.info(f"Userbot запущен как: @{me.username} (ID: {me.id})")

        # Запуск клиента и ожидание завершения
        await asyncio.gather(
            client.run_until_disconnected(),
            shutdown_event.wait()
        )

    except Exception as e:
        logger.exception(f"Ошибка при запуске userbot: {e}")
    finally:
        await client.disconnect()
        kafka_producer.close()
        logger.info("Сервис Userbot завершил работу корректно.")

if __name__ == "__main__":
    try:
        asyncio.run(run_userbot())
    except KeyboardInterrupt:
        logging.getLogger("userbot").info("Userbot остановлен по запросу пользователя.")
    except Exception as e:
        logging.getLogger("userbot").exception(f"Неожиданная ошибка при запуске main: {e}")
