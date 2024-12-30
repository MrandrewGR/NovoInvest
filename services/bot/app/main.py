# File location: ./services/tg_file_bot/app/main.py
import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
import mimetypes
import logging
from kafka import KafkaProducer
import json

from app.config import TELEGRAM_BOT_TOKEN, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.logger import logger


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стартовая команда /start."""
    await update.message.reply_text("Привет! Пришли мне XML-файл, и я передам его в другой сервис.")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик входящих файлов.
    Проверяем, что файл — xml (простая проверка по mime-type или по расширению).
    """
    if not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name
    file_mime_type = document.mime_type

    # Проверка, что файл имеет расширение .xml
    if not file_name.lower().endswith(".xml"):
        await update.message.reply_text("Кажется, это не XML-файл. Попробуй снова.")
        return

    file_id = document.file_id
    file_path_local = os.path.join("files", file_name)

    # Создаем директорию для файлов, если её ещё нет
    os.makedirs("files", exist_ok=True)

    try:
        # Скачиваем файл
        new_file = await context.bot.get_file(file_id)
        await new_file.download_to_drive(file_path_local)
        logger.info(f"Файл сохранён: {file_path_local}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        await update.message.reply_text("Произошла ошибка при сохранении файла.")
        return

    # === Отправка уведомления в Kafka ===
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        message = {
            "file_name": file_name,
            "file_path": file_path_local
        }
        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        logger.info(f"Отправлено сообщение в Kafka: {message}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        await update.message.reply_text("Произошла ошибка при уведомлении сервиса обработки файлов.")
        return

    await update.message.reply_text(
        f"Файл '{file_name}' получен и сохранён. Уведомление отправлено для обработки."
    )


def main():
    """Главная точка входа в приложение."""
    logger.info("Запуск Телеграм-бота для приёма XML-файлов...")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Обработчики команд
    app.add_handler(CommandHandler("start", start_command))

    # Обработчик документов (files)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    logger.info("Бот запущен. Ожидаю сообщений...")
    app.run_polling()


if __name__ == "__main__":
    main()
