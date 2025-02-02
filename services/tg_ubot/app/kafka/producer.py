# services/tg_ubot/app/kafka/producer.py

import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from app.config import settings

logger = logging.getLogger("kafka_producer")


class KafkaMessageProducer:
    """
    Асинхронный KafkaProducer (aiokafka).
    """

    def __init__(self):
        self.logger = logger
        self.producer = None

    async def initialize(self):
        """
        Создаёт AIOKafkaProducer, подключается к брокеру.
        """
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            self.logger.info("AIOKafkaProducer created and connected.")
        except Exception as e:
            self.logger.error(f"Error creating AIOKafkaProducer: {e}")
            raise

    async def send_message(self, topic: str, message: dict):
        """
        Отправляет message в указанный topic (JSON).
        """
        if not self.producer:
            raise Exception("Kafka producer not initialized.")
        try:
            await self.producer.send_and_wait(topic, message)
            self.logger.info(f"Message sent to topic {topic}.")
        except KafkaError as e:
            self.logger.error(f"Error sending message: {e}")
            raise

    async def close(self):
        if self.producer:
            await self.producer.stop()
            self.logger.info("AIOKafkaProducer closed.")
