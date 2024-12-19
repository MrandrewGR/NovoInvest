# File location: services/tg_user_bot/app/main.py

import asyncio
import logging
import os
import signal
import json
from telethon import TelegramClient
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .kafka_consumer import KafkaMessageConsumer
from .handlers.chat_handler import register_chat_handler
from .handlers.channel_handler import register_channel_handler
from .utils import ensure_dir
from .state import MessageCounter

async def run_userbot():
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))

    logger = setup_logging()

    client = TelegramClient('session_name', settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    try:
        kafka_producer = KafkaMessageProducer()
    except Exception:
        logger.critical("Не удалось инициализировать Kafka producer. Завершение работы.")
        return

    try:
        kafka_consumer = KafkaMessageConsumer()
    except Exception:
        logger.critical("Не удалось инициализировать Kafka consumer. Завершение работы.")
        return

    counter = MessageCounter(client)
    userbot_active = asyncio.Event()
    userbot_active.set()

    register_chat_handler(client, kafka_producer, counter, userbot_active)
    register_channel_handler(client, kafka_producer, counter, userbot_active)

    shutdown_event = asyncio.Event()

    def shutdown_signal_handler(signum, frame):
        logger.info(f"Получен сигнал завершения ({signum}), инициирование плавного завершения...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(s, shutdown_signal_handler, s, None)

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

    async def listen_instructions():
        async for message in kafka_consumer.listen():
            try:
                instr = json.loads(message.value)
                await handle_instruction(instr)
            except json.JSONDecodeError:
                logger.error("Не удалось декодировать сообщение инструкции.")
            except Exception as e:
                logger.exception(f"Ошибка при обработке инструкции: {e}")

    try:
        await client.start()

        if not await client.is_user_authorized():
            logger.error("Telegram клиент не авторизован. Проверьте корректность session_name.session.")
            shutdown_event.set()

        me = await client.get_me()
        logger.info(f"Userbot запущен как: @{me.username} (ID: {me.id})")

        await asyncio.gather(
            client.run_until_disconnected(),
            listen_instructions(),
            shutdown_event.wait()
        )

    except Exception as e:
        logger.exception(f"Ошибка при запуске userbot: {e}")
    finally:
        await client.disconnect()
        kafka_producer.close()
        kafka_consumer.close()
        logger.info("Сервис Userbot завершил работу корректно.")

if __name__ == "__main__":
    try:
        asyncio.run(run_userbot())
    except KeyboardInterrupt:
        logging.getLogger("userbot").info("Userbot остановлен по запросу пользователя.")
    except Exception as e:
        logging.getLogger("userbot").exception(f"Неожиданная ошибка при запуске main: {e}")
