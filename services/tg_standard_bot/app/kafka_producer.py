# File location: services/tg_standard_bot/app/kafka_producer.py

import logging
from kafka import KafkaProducer
import json
from .config import settings

logger = logging.getLogger("kafka_producer")

class KafkaMessageProducer:
    def __init__(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer инициализирован.")
        except Exception as e:
            logger.exception(f"Не удалось инициализировать Kafka producer: {e}")
            raise

    def send_instruction(self, instruction: dict):
        try:
            logger.debug(f"Отправка инструкции: {instruction}")
            self.producer.send(settings.KAFKA_INSTRUCTION_TOPIC, value=instruction)
            self.producer.flush()
            logger.info("Инструкция успешно отправлена в Kafka.")
        except Exception as e:
            logger.exception(f"Ошибка при отправке инструкции в Kafka: {e}")

    def close(self):
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer закрыт.")
        except Exception as e:
            logger.exception(f"Ошибка при закрытии Kafka producer: {e}")
