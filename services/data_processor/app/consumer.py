# services/data_processor/app/consumer.py

import os
import json
import asyncio
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from .database import SessionLocal
from .models import TelegramMessage
from .logging_config import setup_logging  # Импортируем функцию настройки логирования
from .config import settings

# Настройка логирования
logger = setup_logging()

def safe_json_deserializer(x):
    if not x:
        logger.warning("Получено пустое сообщение, пропуск.")
        return None
    try:
        return json.loads(x.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e} | Сообщение: {x}")
        return None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def process_message(message: dict, db: Session):
    try:
        msg_id = message.get("id")
        chat_id = message.get("chat_id")
        date = message.get("date")
        content = message.get("original_message", {}).get("message", "")
        media_path = message.get("downloaded_media", "")

        telegram_msg = TelegramMessage(
            message_id=msg_id,
            chat_id=str(chat_id),
            date=date,
            content=content,
            media_path=media_path
        )
        db.add(telegram_msg)
        db.commit()
        db.refresh(telegram_msg)
        logger.info(f"Сохранено сообщение ID {msg_id} в базе данных.")
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения ID {message.get('id')}: {e}")
        db.rollback()

def consume():
    KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC = settings.KAFKA_TOPIC

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='data_processor_group',
            value_deserializer=safe_json_deserializer
        )
        logger.info(f"Запущен потребитель Kafka для топика '{KAFKA_TOPIC}' на серверах {KAFKA_BOOTSTRAP_SERVERS}.")
    except Exception as e:
        logger.exception(f"Не удалось подключиться к Kafka: {e}")
        raise

    for message in consumer:
        try:
            msg = message.value
            if msg is None:
                continue  # Пропуск пустых или невалидных сообщений
            logger.debug(f"Получено сообщение: {msg}")
            # Получаем сессию БД
            db_gen = get_db()
            db = next(db_gen)
            process_message(msg, db)
        except Exception as e:
            logger.exception(f"Ошибка при обработке сообщения: {e}")

if __name__ == "__main__":
    try:
        consume()
    except KeyboardInterrupt:
        logger.info("Потребитель Kafka остановлен вручную.")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка: {e}")
