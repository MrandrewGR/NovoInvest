# File location: services/tg_user_bot/app/kafka_producer.py

import logging
from kafka import KafkaProducer
import json
from .config import settings

logger = logging.getLogger("kafka_producer")

class KafkaMessageProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka producer инициализирован.")

    def send_message(self, topic: str, message: dict):
        try:
            logger.debug(f"Отправка сообщения в топик '{topic}': {message}")
            future = self.producer.send(topic, value=message)
            future.add_callback(self.on_send_success).add_errback(self.on_send_error)
        except Exception as e:
            logger.exception(f"Не удалось отправить сообщение в Kafka: {e}")

    def on_send_success(self, record_metadata):
        logger.info(f"Сообщение успешно отправлено в Kafka: {record_metadata.topic} [{record_metadata.partition}] @ {record_metadata.offset}")

    def on_send_error(self, exc):
        logger.error(f"Ошибка при отправке сообщения в Kafka: {exc}")

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer закрыт.")
