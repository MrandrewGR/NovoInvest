# File location: services/tg_user_bot/app/main.py

import asyncio
import logging
import os
import signal
import json
from telethon import TelegramClient, events
from telethon.errors.rpcbaseerrors import RPCError
from telethon.errors import FloodWaitError
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .kafka_consumer import KafkaMessageConsumer
from .handlers.chat_handler import register_chat_handler
from .handlers.channel_handler import register_channel_handler
from .utils import ensure_dir, serialize_message  # Импортируем serialize_message
from .state import MessageCounter

MAX_BUFFER_SIZE = 10000  # Максимальный размер буфера сообщений
RECONNECT_INTERVAL = 10  # Интервал между попытками переподключения в секундах

async def run_userbot():
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))

    logger = setup_logging()

    client = TelegramClient('session_name', settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    # Изменено на хранение кортежей (topic, message)
    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)

    kafka_producer = None
    kafka_consumer = None

    shutdown_event = asyncio.Event()

    async def initialize_kafka_producer():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            try:
                kafka_producer = KafkaMessageProducer()
                await kafka_producer.initialize()
                logger.info("Kafka producer успешно инициализирован.")
                break
            except Exception as e:
                logger.error(
                    f"Не удалось инициализировать Kafka producer: {e}. Повторная попытка через {RECONNECT_INTERVAL} секунд.")
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
                logger.error(
                    f"Не удалось инициализировать Kafka consumer: {e}. Повторная попытка через {RECONNECT_INTERVAL} секунд.")
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def producer_task():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            if kafka_producer is None:
                await initialize_kafka_producer()
                if kafka_producer is None:
                    continue
            try:
                # Получаем кортеж (topic, message)
                topic, message = await message_buffer.get()
                if message is None:
                    break  # Завершение задачи
                logger.info(f"Отправка сообщения в Kafka топик '{topic}': {message}")
                # Сериализуем сообщение
                serialized_message = serialize_message(message)
                await kafka_producer.send_message(topic, serialized_message)  # Удалено json.dumps
                logger.info("Сообщение успешно отправлено в Kafka.")
                message_buffer.task_done()
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения в Kafka: {e}. Сообщение будет повторно добавлено в буфер.")
                if not message_buffer.full():
                    await message_buffer.put((topic, message))
                else:
                    logger.warning("Буфер сообщений переполнен. Сообщение потеряно.")
                kafka_producer = None  # Сбросить producer для повторной инициализации
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
                logger.error(
                    f"Ошибка в Kafka consumer: {e}. Попытка переподключиться через {RECONNECT_INTERVAL} секунд.")
                kafka_consumer = None
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def fetch_unread_messages():
        # Обработка непрочитанных сообщений в чатах и каналах
        chats = [settings.TELEGRAM_CHAT_ID, settings.TELEGRAM_CHANNEL_ID]
        for chat_id in chats:
            try:
                # Получение последнего обработанного сообщения из состояния
                last_message_id = counter.get_last_message_id(chat_id)

                if last_message_id is not None:
                    logger.debug(f"Последний обработанный message_id для чата {chat_id}: {last_message_id}")
                    async for message in client.iter_messages(chat_id, min_id=last_message_id, reverse=True):
                        # Определяем, в какой топик отправлять сообщение
                        if chat_id == settings.TELEGRAM_CHAT_ID:
                            topic = settings.KAFKA_CHAT_TOPIC
                        elif chat_id == settings.TELEGRAM_CHANNEL_ID:
                            topic = settings.KAFKA_CHANNEL_TOPIC
                        else:
                            topic = settings.KAFKA_TOPIC  # Общий топик для других чатов

                        await message_buffer.put((topic, message.to_dict()))
                        logger.info(f"Непрочитанное сообщение добавлено в буфер: {message.id} из чата {chat_id} в топик '{topic}'")
                        counter.update_last_message_id(chat_id, message.id)
                else:
                    logger.debug(
                        f"Нет сохраненного last_message_id для чата {chat_id}. Загрузка последних 100 сообщений.")
                    async for message in client.iter_messages(chat_id, reverse=True, limit=100):
                        # Определяем, в какой топик отправлять сообщение
                        if chat_id == settings.TELEGRAM_CHAT_ID:
                            topic = settings.KAFKA_CHAT_TOPIC
                        elif chat_id == settings.TELEGRAM_CHANNEL_ID:
                            topic = settings.KAFKA_CHANNEL_TOPIC
                        else:
                            topic = settings.KAFKA_TOPIC  # Общий топик для других чатов

                        await message_buffer.put((topic, message.to_dict()))
                        logger.info(f"Непрочитанное сообщение добавлено в буфер: {message.id} из чата {chat_id} в топик '{topic}'")
                        counter.update_last_message_id(chat_id, message.id)
            except RPCError as e:
                logger.error(f"Ошибка при получении сущности для чата {chat_id}: {e}")
            except Exception as e:
                logger.exception(f"Ошибка при загрузке непрочитанных сообщений из чата {chat_id}: {e}")

    counter = MessageCounter(client)
    userbot_active = asyncio.Event()
    userbot_active.set()

    def shutdown_signal_handler(signum, frame):
        logger.info(f"Получен сигнал завершения ({signum}), инициирование плавного завершения...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        try:
            loop.add_signal_handler(s, shutdown_signal_handler, s, None)
        except NotImplementedError:
            # Windows не поддерживает добавление обработчиков сигналов из asyncio
            pass

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

    try:
        await client.start()

        if not await client.is_user_authorized():
            logger.error("Telegram клиент не авторизован. Проверьте корректность session_name.session.")
            shutdown_event.set()
            return

        me = await client.get_me()
        logger.info(f"Userbot запущен как: @{me.username} (ID: {me.id})")

        # Регистрация обработчиков после авторизации
        register_chat_handler(client, message_buffer, counter, userbot_active)
        register_channel_handler(client, message_buffer, counter, userbot_active)

        # Загрузка непрочитанных сообщений
        await fetch_unread_messages()

        # Создание асинхронных задач для producer и consumer
        producer = asyncio.create_task(producer_task())
        consumer = asyncio.create_task(consumer_task())

        await asyncio.gather(
            client.run_until_disconnected(),
            consumer,
            producer,
            shutdown_event.wait()
        )

    except Exception as e:
        logger.exception(f"Ошибка при запуске userbot: {e}")
    finally:
        await client.disconnect()
        if kafka_producer:
            await kafka_producer.close()
            logger.info("Kafka producer закрыт.")
        if kafka_consumer:
            await kafka_consumer.close()
            logger.info("Kafka consumer закрыт.")
        logger.info("Сервис Userbot завершил работу корректно.")

if __name__ == "__main__":
    try:
        asyncio.run(run_userbot())
    except KeyboardInterrupt:
        logging.getLogger("userbot").info("Userbot остановлен по запросу пользователя.")
    except Exception as e:
        logging.getLogger("userbot").exception(f"Неожиданная ошибка при запуске main: {e}")
