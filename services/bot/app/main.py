import os
import time
import json
import threading
import asyncio

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)
from kafka import KafkaProducer, KafkaConsumer
import logging

from app.config import (
    TELEGRAM_BOT_TOKEN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_RESULT_TOPIC,
    FILES_DIR,
    ALLOWED_USER_IDS,
)
from app.logger import logger

# Словарь для хранения соответствий user_id и chat_id
USER_CHAT_MAP = {}

async def start_command(update: Update, context):
    """Обработка команды /start."""
    user_id = update.effective_user.id
    USER_CHAT_MAP[str(user_id)] = update.effective_chat.id
    await update.message.reply_text("Привет! Пришли мне XML-файл, и я передам его на обработку.")

async def handle_file(update: Update, context):
    """
    Обработка входящих файлов.
    Проверяем формат XML и вайтлист пользователей, затем переименовываем файл и сохраняем.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    USER_CHAT_MAP[str(user_id)] = chat_id

    if str(user_id) not in ALLOWED_USER_IDS:
        logger.warning(f"Пользователь {user_id} не авторизован для отправки файлов.")
        await update.message.reply_text("У вас нет разрешения отправлять файлы этому боту.")
        return

    if not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name

    # Проверка формата файла
    if not file_name.lower().endswith(".xml"):
        await update.message.reply_text("Кажется, это не XML-файл. Попробуй снова.")
        return

    file_id = document.file_id
    ts = int(time.time())
    new_file_name = f"{user_id}_{ts}.xml"
    file_path_local = os.path.join(FILES_DIR, new_file_name)

    os.makedirs(FILES_DIR, exist_ok=True)

    try:
        new_file = await context.bot.get_file(file_id)
        await new_file.download_to_drive(file_path_local)
        logger.info(f"Файл сохранён: {file_path_local}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        await update.message.reply_text("Произошла ошибка при сохранении файла.")
        return

    # Отправка уведомления в Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        message = {"user_id": str(user_id), "file_path": file_path_local}
        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        logger.info(f"Сообщение отправлено в Kafka: {message}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        await update.message.reply_text("Ошибка при уведомлении сервиса обработки файлов.")
        return

    await update.message.reply_text(
        f"Файл '{file_name}' получен и сохранён как '{new_file_name}'. Отправлен на обработку."
    )

def consume_results_from_kafka(application, loop):
    """
    Kafka Consumer для получения результатов обработки файлов.
    Работает в отдельном потоке, использует event loop из основного потока.
    """
    consumer = KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bot_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    logger.info(f"Kafka Consumer запущен для топика: {KAFKA_RESULT_TOPIC}")

    for msg in consumer:
        try:
            data = msg.value
            user_id = data.get("user_id")
            result = data.get("result", [])
            chat_id = USER_CHAT_MAP.get(str(user_id))

            if not chat_id:
                logger.warning(f"Не удалось найти chat_id для user_id={user_id}.")
                continue

            if not result:
                text_msg = "Обработчик не вернул результатов. Возможно, файл пуст."
            else:
                text_msg = "Результаты обработки:\n" + "\n".join(
                    [f"- {item.get('name')} (ISIN: {item.get('isin')}): {item.get('quantity')}" for item in result]
                )

            # Запускаем отправку сообщения в основном event loop
            asyncio.run_coroutine_threadsafe(
                application.bot.send_message(chat_id=chat_id, text=text_msg),
                loop
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения из Kafka: {e}")

def main():
    """Главная точка входа."""
    logger.info("Запуск Telegram-бота.")

    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # Создаём event loop
    loop = asyncio.get_event_loop()

    # Запускаем Kafka Consumer в отдельном потоке
    kafka_thread = threading.Thread(
        target=consume_results_from_kafka,
        args=(application, loop),
        daemon=True
    )
    kafka_thread.start()

    application.run_polling()

if __name__ == "__main__":
    main()
