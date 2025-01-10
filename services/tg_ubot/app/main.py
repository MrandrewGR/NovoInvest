# File location: services/tg_ubot/app/main.py

import asyncio
import logging
import os
import signal
import json
from telethon import TelegramClient
from telethon.errors.rpcbaseerrors import RPCError
from telethon.errors import FloodWaitError
from telethon.tl.types import Dialog
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .kafka_consumer import KafkaMessageConsumer
from .handlers.unified_handler import register_unified_handler
from .utils import ensure_dir
from .state import MessageCounter

MAX_BUFFER_SIZE = 10000
RECONNECT_INTERVAL = 10

async def run_userbot():
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))

    logger = setup_logging()

    session_file = os.getenv('SESSION_FILE', 'session_name.session')
    client = TelegramClient(session_file, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)
    kafka_producer = None
    kafka_consumer = None
    shutdown_event = asyncio.Event()

    # Сигналы для корректного завершения
    def shutdown_signal_handler(signum, frame):
        logger.info(f"Получен сигнал завершения ({signum}), инициируем плавное завершение...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        try:
            loop.add_signal_handler(s, shutdown_signal_handler, s, None)
        except NotImplementedError:
            pass

    userbot_active = asyncio.Event()
    userbot_active.set()
    counter = MessageCounter(client)

    async def handle_instruction(instruction):
        action = instruction.get("action")
        logger.info(f"Получена инструкция: {action}")
        if action == "start":
            userbot_active.set()
            logger.info("Userbot активирован.")
        elif action == "stop":
            userbot_active.clear()
            logger.info("Userbot приостановлен.")
        else:
            logger.warning(f"Неизвестная инструкция: {action}")

    # -- Инициализация Kafka
    async def initialize_kafka_producer():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            try:
                kafka_producer = KafkaMessageProducer()
                await kafka_producer.initialize()
                logger.info("Kafka producer успешно инициализирован.")
                break
            except Exception as e:
                logger.error(f"Не удалось инициализировать Kafka producer: {e}. Повтор через {RECONNECT_INTERVAL}с.")
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def producer_task():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            if kafka_producer is None:
                await initialize_kafka_producer()
                if kafka_producer is None:
                    continue
            try:
                topic, message = await message_buffer.get()
                if message is None:
                    break
                logger.info(f"Отправка сообщения в Kafka топик '{topic}'")
                await kafka_producer.send_message(topic, message)
                logger.info("Сообщение успешно отправлено в Kafka.")
                message_buffer.task_done()
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
                kafka_producer = None
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def initialize_kafka_consumer():
        nonlocal kafka_consumer
        while not shutdown_event.is_set():
            try:
                kafka_consumer = KafkaMessageConsumer()
                await kafka_consumer.initialize()
                logger.info("Kafka consumer успешно инициализирован.")
                break
            except Exception as e:
                logger.error(f"Не удалось инициализировать Kafka consumer: {e}. Повтор через {RECONNECT_INTERVAL}с.")
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def consumer_task():
        nonlocal kafka_consumer
        while not shutdown_event.is_set():
            if kafka_consumer is None:
                await initialize_kafka_consumer()
                if kafka_consumer is None:
                    continue
            try:
                async for message in kafka_consumer.listen():
                    try:
                        instr = json.loads(message.value.decode('utf-8'))
                        logger.info(f"Получена инструкция из Kafka: {instr}")
                        await handle_instruction(instr)
                    except json.JSONDecodeError:
                        logger.error("Не удалось декодировать сообщение инструкции.")
                    except Exception as e:
                        logger.exception(f"Ошибка при обработке инструкции: {e}")
            except Exception as e:
                logger.error(f"Ошибка в Kafka consumer: {e}. Повтор через {RECONNECT_INTERVAL}с.")
                kafka_consumer = None
                await asyncio.sleep(RECONNECT_INTERVAL)

    # -- Основной блок
    try:
        # 1. Запускаем userbot
        await client.start()
        if not await client.is_user_authorized():
            logger.error("Telegram клиент не авторизован (session_file некорректен?).")
            shutdown_event.set()
            return

        # 2. Принудительно загрузим все диалоги, чтобы Telethon видел юзеров и чаты
        logger.info("Загружаем диалоги (get_dialogs), чтобы Telethon узнал о всех пользователях и чатах...")
        await client.get_dialogs()

        # 3. Формируем словарь chat_id -> словарь
        chat_id_to_data = {}
        for original_id in settings.TELEGRAM_TARGET_IDS:
            try:
                entity = await client.get_entity(original_id)
                # попытаемся получить "title" или "username" или fallback
                chat_title = getattr(entity, "title", "") or getattr(entity, "username", "") or str(original_id)
                # Сохраним словарь (можно добавить больше полей)
                chat_id_to_data[original_id] = {
                    "chat_id": original_id,
                    "target_id": original_id,
                    "chat_title": chat_title,
                }
                logger.info(f"Сопоставление для {original_id}: {chat_id_to_data[original_id]}")
            except Exception as e:
                logger.error(f"Не удалось получить entity для {original_id}: {e}")
                # Запишем какую-то заглушку, чтобы не падать
                chat_id_to_data[original_id] = {
                    "chat_id": original_id,
                    "target_id": "unknown",
                    "chat_title": f"Unknown_{original_id}"
                }

        me = await client.get_me()
        logger.info(f"Userbot запущен как: @{me.username} (ID: {me.id})")

        # 4. Регистрируем unified_handler, передавая chat_id_to_data (словарь)
        register_unified_handler(client, message_buffer, counter, userbot_active, chat_id_to_data)

        # 5. Создаём задачи: продьюсер, консюмер и сам Telethon
        producer = asyncio.create_task(producer_task())
        # consumer = asyncio.create_task(consumer_task())  # если нужно

        # 6. Запускаем всё вместе
        await asyncio.gather(
            client.run_until_disconnected(),
            producer,
            shutdown_event.wait()
        )

    except Exception as e:
        logger.exception(f"Ошибка при запуске userbot: {e}")
    finally:
        # Плавное завершение
        await client.disconnect()
        if kafka_producer:
            await kafka_producer.close()
            logger.info("Kafka producer закрыт.")
        if kafka_consumer:
            await kafka_consumer.close()
            logger.info("Kafka consumer закрыт.")
        logger.info("Сервис tg_ubot завершил работу корректно.")

if __name__ == "__main__":
    try:
        asyncio.run(run_userbot())
    except KeyboardInterrupt:
        logging.getLogger("userbot").info("Userbot остановлен по запросу пользователя.")
    except Exception as e:
        logging.getLogger("userbot").exception(f"Неожиданная ошибка при запуске main: {e}")
