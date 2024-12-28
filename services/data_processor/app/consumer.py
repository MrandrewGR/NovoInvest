# File: services/data_processor/app/consumer.py

import json
import logging
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from .database import SessionLocal
from .models import TgUbot
from .logging_config import setup_logging
from .config import settings

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
    # Здесь логика записи в БД
    try:
        msg_id = message.get("id")
        chat_id = message.get("chat_id")
        date = message.get("date")  # Строка даты, убедитесь, что она парсится
        content = message.get("original_message", {}).get("message", "")
        media_path = message.get("downloaded_media", "")

        record = TgUbot(
            message_id=msg_id,
            chat_id=str(chat_id),
            date=date,
            content=content,
            media_path=media_path
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        logger.info(f"Сохранено сообщение ID={msg_id} в таблицу tg_ubot.")
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения ID={message.get('id')}: {e}")
        db.rollback()

def consume():
    KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
    # Вместо старого топика читаем KAFKA_UBOT_OUTPUT_TOPIC
    KAFKA_TOPIC = settings.KAFKA_UBOT_OUTPUT_TOPIC

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='data_processor_group',
            value_deserializer=safe_json_deserializer
        )
        logger.info(f"Запущен KafkaConsumer для топика '{KAFKA_TOPIC}' на {KAFKA_BOOTSTRAP_SERVERS}.")
    except Exception as e:
        logger.exception(f"Не удалось подключиться к Kafka: {e}")
        raise

    for message in consumer:
        try:
            msg = message.value
            if msg is None:
                continue  # Пропуск пустых или невалидных сообщений
            logger.debug(f"Получено сообщение: {msg}")

            db_gen = get_db()
            db = next(db_gen)
            process_message(msg, db)
        except Exception as e:
            logger.exception(f"Ошибка при обработке сообщения: {e}")
        finally:
            # Закрываем сессию
            try:
                db_gen.close()
            except:
                pass

if __name__ == "__main__":
    try:
        consume()
    except KeyboardInterrupt:
        logger.info("Потребитель Kafka остановлен вручную.")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка: {e}")
