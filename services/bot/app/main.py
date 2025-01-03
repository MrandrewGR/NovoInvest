# File location: ./services/bot/app/main.py

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
    ContextTypes
)
from kafka import KafkaProducer, KafkaConsumer
import logging

from app.config import (
    TELEGRAM_BOT_TOKEN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_RESULT_TOPIC,
    FILES_DIR,
    ALLOWED_USER_IDS
)
from app.logger import logger

# Словарь, где ключ: user_id (str), значение: chat_id (int)
USER_CHAT_MAP = {}


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стартовая команда /start."""
    user_id = update.effective_user.id
    USER_CHAT_MAP[str(user_id)] = update.effective_chat.id
    await update.message.reply_text("Привет! Пришли мне XML-файл, и я передам его на обработку.")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик входящих файлов.
    Проверяем, что файл — xml и пользователь есть в вайтлисте.
    Переименовываем файл в user_id_timestamp.xml.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    USER_CHAT_MAP[str(user_id)] = chat_id

    if str(user_id) not in ALLOWED_USER_IDS:
        logger.warning(f"Пользователь с ID {user_id} попытался отправить файл без разрешения.")
        await update.message.reply_text("У вас нет разрешения отправлять файлы этому боту.")
        return

    if not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name

    # Проверка, что файл действительно .xml
    if not file_name.lower().endswith(".xml"):
        await update.message.reply_text("Кажется, это не XML-файл. Попробуй снова.")
        return

    file_id = document.file_id

    # Переименовываем файл -> user_id_timestamp.xml
    ts = int(time.time())
    new_file_name = f"{user_id}_{ts}.xml"
    file_path_local = os.path.join(FILES_DIR, new_file_name)

    # Создаем директорию для файлов, если её ещё нет
    os.makedirs(FILES_DIR, exist_ok=True)

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
            "user_id": str(user_id),
            "file_path": file_path_local
        }
        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        logger.info(f"Отправлено сообщение в Kafka (топик={KAFKA_TOPIC}): {message}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        await update.message.reply_text("Произошла ошибка при уведомлении сервиса обработки файлов.")
        return

    await update.message.reply_text(
        f"Файл '{file_name}' получен и сохранён как '{new_file_name}'. Отправил на обработку..."
    )


def consume_results_from_kafka(application):
    """
    Запускается в отдельном потоке — слушаем топик KAFKA_RESULT_TOPIC.
    Когда приходят данные об обработке (isin_in_portfolio),
    находим нужного пользователя и отправляем ему сообщение через event loop приложения.
    """
    consumer = KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='bot_consumer_group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    logger.info(f"[bot] Consumer запущен, слушаем топик '{KAFKA_RESULT_TOPIC}'...")

    for message in consumer:
        msg_str = message.value
        logger.info(f"[bot] Получено сообщение из Kafka (топик={KAFKA_RESULT_TOPIC}): {msg_str}")

        try:
            msg_json = json.loads(msg_str)
        except json.JSONDecodeError:
            logger.exception("Некорректный JSON в сообщении.")
            continue

        user_id = msg_json.get("user_id")
        result = msg_json.get("result", [])

        if not user_id:
            logger.warning("Не указан user_id в результатах. Пропускаем.")
            continue

        # Находим chat_id
        chat_id = USER_CHAT_MAP.get(str(user_id))
        if not chat_id:
            logger.warning(f"Не найден chat_id для user_id={user_id}. Возможно, бот был перезапущен.")
            continue

        # Формируем текст для отправки
        if not result:
            text_msg = "Обработчик не вернул позиций по сделкам, видимо, пустой отчёт."
        else:
            lines = ["Результаты обработки XML:"]
            for item in result:
                isin = item.get("isin")
                qty = item.get("quantity")
                avgp = item.get("avg_price")
                lines.append(f"- ISIN: {isin}, Кол-во: {qty}, Средняя цена: {avgp}")
            text_msg = "\n".join(lines)

        # Формируем корутину, которая отправит сообщение в чат Telegram
        async def send_result():
            try:
                await application.bot.send_message(chat_id=chat_id, text=text_msg)
                logger.info(f"Отправили результаты пользователю {user_id} (chat_id={chat_id}).")
            except Exception as e:
                logger.exception(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

        # Берём event loop из application (v20+)
        loop = application.asyncio_loop
        # Запускаем корутину в event loop (а не create_task из другого потока)
        asyncio.run_coroutine_threadsafe(send_result(), loop)


def main():
    """Главная точка входа в приложение (бот)."""
    logger.info("Запуск Телеграм-бота для приёма XML-файлов...")

    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Обработчики команд
    application.add_handler(CommandHandler("start", start_command))

    # Обработчик документов (files)
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # Запускаем поток-потребитель Kafka
    consumer_thread = threading.Thread(
        target=consume_results_from_kafka,
        args=(application,),
        daemon=True
    )
    consumer_thread.start()

    logger.info("Бот запущен. Ожидаю сообщений...")
    application.run_polling()


if __name__ == "__main__":
    main()
