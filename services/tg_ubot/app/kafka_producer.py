# services/tg_ubot/app/kafka_producer.py

import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from .config import settings

logger = logging.getLogger("kafka_producer")


class KafkaMessageProducer:
    def __init__(self):
        self.logger = logging.getLogger("kafka_producer")
        self.producer = None

    async def initialize(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            self.logger.info("AIOKafkaProducer created and connected to broker.")
        except Exception as e:
            self.logger.error(f"Error creating AIOKafkaProducer: {e}")
            raise

    async def send_message(self, topic, message):
        if self.producer is None:
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
