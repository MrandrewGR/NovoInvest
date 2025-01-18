import os
import time
import json
import asyncio

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import (
    TELEGRAM_BOT_TOKEN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_RESULT_TOPIC,
    FILES_DIR,
    ALLOWED_USER_IDS
)
from app.logger import logger

# Dictionary mapping user_id (str) to chat_id (int)
USER_CHAT_MAP = {}

# Retry configuration for sending messages to Telegram
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
async def send_message_with_retry(bot, chat_id: int, text: str):
    await bot.send_message(chat_id=chat_id, text=text)
    logger.info(f"Отправлено сообщение пользователю {chat_id}: {text}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command."""
    user_id = update.effective_user.id
    USER_CHAT_MAP[str(user_id)] = update.effective_chat.id
    try:
        await send_message_with_retry(context.bot, update.effective_chat.id, "Привет! Пришли мне XML-файл, и я передам его на обработку.")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения /start пользователю {user_id}: {e}")

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle incoming files.
    Validate that the file is XML and the user is whitelisted.
    Rename the file to user_id_timestamp.xml.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    USER_CHAT_MAP[str(user_id)] = chat_id

    if str(user_id) not in ALLOWED_USER_IDS:
        logger.warning(f"Пользователь с ID {user_id} попытался отправить файл без разрешения.")
        try:
            await send_message_with_retry(context.bot, chat_id, "У вас нет разрешения отправлять файлы этому боту.")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения о отсутствии разрешений пользователю {user_id}: {e}")
        return

    if not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name

    # Check if the file is indeed .xml
    if not file_name.lower().endswith(".xml"):
        try:
            await send_message_with_retry(context.bot, chat_id, "Кажется, это не XML-файл. Попробуй снова.")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения о неверном формате файла пользователю {user_id}: {e}")
        return

    file_id = document.file_id

    # Rename the file -> user_id_timestamp.xml
    ts = int(time.time())
    new_file_name = f"{user_id}_{ts}.xml"
    file_path_local = os.path.join(FILES_DIR, new_file_name)

    # Create the directory for files if it doesn't exist
    os.makedirs(FILES_DIR, exist_ok=True)

    try:
        # Download the file
        new_file = await context.bot.get_file(file_id)
        await new_file.download_to_drive(file_path_local)
        logger.info(f"Файл сохранён: {file_path_local}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        try:
            await send_message_with_retry(context.bot, chat_id, "Произошла ошибка при сохранении файла.")
        except Exception as send_err:
            logger.error(f"Ошибка при отправке сообщения об ошибке сохранения файла пользователю {user_id}: {send_err}")
        return

    # === Send notification to Kafka ===
    try:
        message = {
            "user_id": str(user_id),
            "file_path": file_path_local
        }
        await context.bot_data['producer'].send_and_wait(KAFKA_TOPIC, json.dumps(message).encode('utf-8'))
        logger.info(f"Отправлено сообщение в Kafka (топик={KAFKA_TOPIC}): {message}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        try:
            await send_message_with_retry(context.bot, chat_id, "Произошла ошибка при уведомлении сервиса обработки файлов.")
        except Exception as send_err:
            logger.error(f"Ошибка при отправке сообщения об ошибке уведомления в Kafka пользователю {user_id}: {send_err}")
        return

    try:
        await send_message_with_retry(
            context.bot,
            chat_id,
            f"Файл '{file_name}' получен и сохранён как '{new_file_name}'. Отправил на обработку..."
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке подтверждения пользователю {user_id}: {e}")

async def consume_results_from_kafka(application):
    """
    Asynchronous Kafka consumer that listens for processing results and sends them to users.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='bot_consumer_group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    # Connect to Kafka
    await consumer.start()
    logger.info(f"[bot] Consumer запущен, слушаем топик '{KAFKA_RESULT_TOPIC}'...")

    try:
        async for msg in consumer:
            msg_str = msg.value
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

            chat_id = USER_CHAT_MAP.get(str(user_id))
            if not chat_id:
                logger.warning(f"Не найден chat_id для user_id={user_id}. Возможно, бот был перезапущен.")
                continue

            if not result:
                text_msg = "Обработчик не вернул позиций по сделкам, возможно, отчёт пуст."
            else:
                lines = ["Результаты обработки XML:"]
                for item in result:
                    isin = item.get("isin")
                    name = item.get("name")
                    qty = item.get("quantity")
                    lines.append(f"- ISIN: {isin}, Название: {name}, Кол-во: {qty}")
                text_msg = "\n".join(lines)

            try:
                await send_message_with_retry(application.bot, chat_id, text_msg)
                logger.info(f"Отправили результаты пользователю {user_id} (chat_id={chat_id}).")
            except Exception as e:
                logger.exception(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

    finally:
        await consumer.stop()
        logger.info("Kafka Consumer остановлен.")

async def startup(application: ContextTypes.DEFAULT_TYPE):
    """Startup function called when the bot starts."""
    # Create Kafka Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v  # Already serialized bytes
    )
    await producer.start()
    application.bot_data['producer'] = producer
    logger.info("Kafka Producer запущен.")

    # Start Kafka Consumer as a background task
    consumer_task = asyncio.create_task(consume_results_from_kafka(application))
    application.bot_data['consumer_task'] = consumer_task
    logger.info("Kafka Consumer задача запущена.")

async def shutdown(application: ContextTypes.DEFAULT_TYPE):
    """Shutdown function called when the bot stops."""
    # Stop Kafka Producer
    producer: AIOKafkaProducer = application.bot_data.get('producer')
    if producer:
        await producer.stop()
        logger.info("Kafka Producer остановлен.")

    # Stop Kafka Consumer
    consumer_task: asyncio.Task = application.bot_data.get('consumer_task')
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("Kafka Consumer задача отменена.")

    # Stop the Telegram application
    await application.stop()
    await application.shutdown()
    logger.info("Бот полностью остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Получен сигнал завершения. Останавливаю бота...")
