# File location: services/tg_standard_bot/app/main.py

import asyncio
import logging
import os
import signal
import json
from telegram.ext import ApplicationBuilder, CommandHandler
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .handlers import InstructionHandler

MAX_BUFFER_SIZE = 10000  # Максимальный размер буфера сообщений
RECONNECT_INTERVAL = 5  # Интервал между попытками переподключения в секундах


async def main():
    ensure_dir(settings.LOG_DIR)  # Предполагается, что у вас есть функция ensure_dir
    logger = setup_logging()
    logger.info("Запуск стандартного Telegram бота.")

    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)

    kafka_producer = None

    shutdown_event = asyncio.Event()

    async def initialize_kafka_producer():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            try:
                kafka_producer = KafkaMessageProducer()
                logger.info("Kafka producer успешно инициализирован.")
                break
            except Exception as e:
                logger.error(
                    f"Не удалось инициализировать Kafka producer: {e}. Повторная попытка через {RECONNECT_INTERVAL} секунд.")
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def producer_task():
        nonlocal kafka_producer
        while not shutdown_event.is_set():
            if kafka_producer is None:
                await initialize_kafka_producer()
                if kafka_producer is None:
                    continue
            try:
                message = await message_buffer.get()
                if message is None:
                    break  # Завершение задачи
                kafka_producer.send(message)
                message_buffer.task_done()
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения в Kafka: {e}. Сообщение будет повторно добавлено в буфер.")
                if not message_buffer.full():
                    await message_buffer.put(message)
                else:
                    logger.warning("Буфер сообщений переполнен. Сообщение потеряно.")
                kafka_producer = None  # Сбросить producer для повторной инициализации
                await asyncio.sleep(RECONNECT_INTERVAL)

    def shutdown_signal_handler():
        logger.info("Инициирование завершения работы бота...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_signal_handler)

    # Инициализация Kafka producer
    await initialize_kafka_producer()

    # Создание асинхронной задачи для отправки сообщений
    producer = asyncio.create_task(producer_task())

    # Инициализация Telegram приложения
    application = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build()

    # Инициализация обработчика с буфером сообщений
    instruction_handler = InstructionHandler(message_buffer)

    # Добавление обработчиков команд
    application.add_handler(CommandHandler("start_instruction", instruction_handler.start_instruction))
    application.add_handler(CommandHandler("stop_instruction", instruction_handler.stop_instruction))
    application.add_handler(
        CommandHandler("add_instruction", instruction_handler.add_instruction_command, pass_args=True))

    # Создание асинхронной задачи для запуска Telegram бота
    polling_task = asyncio.create_task(application.run_polling())

    # Ожидание события завершения
    await shutdown_event.wait()

    # Завершение работы Telegram приложения
    await application.shutdown()
    await application.stop()

    # Завершение задачи producer
    await message_buffer.put(None)  # Отправка сигнала завершения
    await producer

    # Закрытие Kafka producer
    if kafka_producer:
        kafka_producer.close()

    logger.info("Бот завершил работу корректно.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.getLogger("tg_standard_bot").info("Бот остановлен по запросу.")
    except Exception as e:
        logging.getLogger("tg_standard_bot").exception(f"Неожиданная ошибка: {e}")
