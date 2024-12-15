# app/consumer.py
import os
import json
import logging
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from .database import SessionLocal, engine
from data_processor.app.models import Base, TelegramMessage

# Настройка логирования
LOG_FILE = os.getenv("LOG_FILE", "/app/logs/data_processor.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    filename=LOG_FILE,
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

# Создание таблиц в базе данных
Base.metadata.create_all(bind=engine)

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
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_channel_messages")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='data_processor_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logger.info(f"Запущен потребитель Kafka для топика '{KAFKA_TOPIC}'.")

    for message in consumer:
        msg = message.value
        db_gen = get_db()
        db = next(db_gen)
        process_message(msg, db)

if __name__ == "__main__":
    try:
        consume()
    except KeyboardInterrupt:
        logger.info("Потребитель Kafka остановлен вручную.")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка: {e}")
