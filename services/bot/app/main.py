import os
import time
import json
import asyncio

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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

# Повторная отправка сообщений в Telegram
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
async def send_message_with_retry(bot, chat_id: int, text: str):
    await bot.send_message(chat_id=chat_id, text=text)
    logger.info(f"Отправлено сообщение пользователю {chat_id}: {text}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start."""
    user_id = update.effective_user.id
    USER_CHAT_MAP[str(user_id)] = update.effective_chat.id
    try:
        await send_message_with_retry(
            context.bot, update.effective_chat.id, "Привет! Пришли мне XML-файл, и я передам его на обработку."
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения /start пользователю {user_id}: {e}")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка входящих файлов.
    Проверяем формат XML и вайтлист пользователей, затем переименовываем файл и сохраняем.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    USER_CHAT_MAP[str(user_id)] = chat_id

    if str(user_id) not in ALLOWED_USER_IDS:
        logger.warning(f"Пользователь {user_id} не авторизован для отправки файлов.")
        await send_message_with_retry(
            context.bot, chat_id, "У вас нет разрешения отправлять файлы этому боту."
        )
        return

    if not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name

    # Проверка формата файла
    if not file_name.lower().endswith(".xml"):
        await send_message_with_retry(
            context.bot, chat_id, "Кажется, это не XML-файл. Попробуй снова."
        )
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
        await send_message_with_retry(
            context.bot, chat_id, "Произошла ошибка при сохранении файла."
        )
        return

    # Отправка уведомления в Kafka
    try:
        message = {"user_id": str(user_id), "file_path": file_path_local}
        await context.bot_data["producer"].send_and_wait(
            KAFKA_TOPIC, json.dumps(message).encode("utf-8")
        )
        logger.info(f"Сообщение отправлено в Kafka: {message}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        await send_message_with_retry(
            context.bot, chat_id, "Ошибка при уведомлении сервиса обработки файлов."
        )
        return

    await send_message_with_retry(
        context.bot,
        chat_id,
        f"Файл '{file_name}' получен и сохранён как '{new_file_name}'. Отправлен на обработку.",
    )


async def consume_results_from_kafka(application):
    """Kafka Consumer для получения результатов обработки файлов."""
    consumer = AIOKafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="bot_consumer_group",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    await consumer.start()
    logger.info(f"Kafka Consumer запущен, топик: {KAFKA_RESULT_TOPIC}")

    try:
        async for msg in consumer:
            msg_str = msg.value
            logger.info(f"Получено сообщение из Kafka: {msg_str}")

            try:
                msg_json = json.loads(msg_str)
            except json.JSONDecodeError:
                logger.error("Некорректный JSON в сообщении Kafka.")
                continue

            user_id = msg_json.get("user_id")
            result = msg_json.get("result", [])
            chat_id = USER_CHAT_MAP.get(str(user_id))

            if not user_id or not chat_id:
                logger.warning(f"Пропущено сообщение Kafka: user_id={user_id}, chat_id={chat_id}")
                continue

            if not result:
                text_msg = "Обработчик не вернул результатов. Возможно, файл пуст."
            else:
                text_msg = "Результаты обработки:\n" + "\n".join(
                    [f"- {r.get('name')} (ISIN: {r.get('isin')}): {r.get('quantity')}" for r in result]
                )

            await send_message_with_retry(application.bot, chat_id, text_msg)
    finally:
        await consumer.stop()
        logger.info("Kafka Consumer остановлен.")


async def main():
    """Главная точка входа."""
    logger.info("Запуск Telegram-бота для приёма XML-файлов.")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # Инициализация Kafka Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    application.bot_data["producer"] = producer

    # Запуск Kafka Consumer в фоновом режиме
    consumer_task = asyncio.create_task(consume_results_from_kafka(application))

    try:
        # Используем run_polling в основном цикле приложения
        await application.run_polling()
    finally:
        # Завершаем задачи при остановке приложения
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("Kafka Consumer task was cancelled.")
        await producer.stop()
        logger.info("Бот завершил работу.")


if __name__ == "__main__":
    # Запускаем событийный цикл напрямую
    asyncio.run(main())

