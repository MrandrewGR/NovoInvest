# File location: services/tg_user_bot/app/kafka_producer.py

import logging
import json
from aiokafka import AIOKafkaProducer
from .config import settings

logger = logging.getLogger("kafka_producer")

class KafkaMessageProducer:
    def __init__(self):
        self.producer = None

    async def initialize(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        logger.info("Kafka producer инициализирован.")

    async def send_message(self, topic: str, message: dict):
        try:
            logger.debug(f"Отправка сообщения в топик '{topic}': {message}")
            await self.producer.send_and_wait(topic, value=message)
            logger.info("Сообщение успешно отправлено в Kafka.")
        except Exception as e:
            logger.exception(f"Не удалось отправить сообщение в Kafka: {e}")

    async def close(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer закрыт.")
