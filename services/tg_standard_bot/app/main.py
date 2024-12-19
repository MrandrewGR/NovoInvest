# File location: services/tg_standard_bot/app/main.py

import logging
import asyncio
import signal
from telegram.ext import ApplicationBuilder, CommandHandler
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .handlers import InstructionHandler

async def main():
    logger = setup_logging()
    logger.info("Запуск стандартного Telegram бота.")

    try:
        kafka_producer = KafkaMessageProducer()
    except Exception as e:
        logger.critical(f"Не удалось инициализировать Kafka producer: {e}")
        return

    application = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build()

    instruction_handler = InstructionHandler(kafka_producer)

    application.add_handler(CommandHandler("start_instruction", instruction_handler.start_instruction))
    application.add_handler(CommandHandler("stop_instruction", instruction_handler.stop_instruction))
    application.add_handler(CommandHandler("add_instruction", instruction_handler.add_instruction_command, pass_args=True))

    shutdown_event = asyncio.Event()

    def shutdown_signal_handler():
        logger.info("Инициирование завершения работы бота...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_signal_handler)

    task = asyncio.create_task(application.run_polling())

    await shutdown_event.wait()

    await application.shutdown()
    await application.stop()
    kafka_producer.close()
    logger.info("Бот завершил работу корректно.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.getLogger("tg_standard_bot").info("Бот остановлен по запросу.")
    except Exception as e:
        logging.getLogger("tg_standard_bot").exception(f"Неожиданная ошибка: {e}")
