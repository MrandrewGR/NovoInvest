# services/tg_ubot/app/kafka_consumer.py

import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from .config import settings

logger = logging.getLogger("kafka_consumer")

class KafkaMessageConsumer:
    def __init__(self, topics, group_id):
        self.logger = logging.getLogger("kafka_consumer")
        self.topics = topics
        self.group_id = group_id
        self.consumer = None

    async def initialize(self):
        try:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode('utf-8')
            )
            await self.consumer.start()
            self.logger.info(f"AIOKafkaConsumer initialized, listening on: {', '.join(self.topics)}")
        except Exception as e:
            self.logger.error(f"Error initializing AIOKafkaConsumer: {e}")
            raise

    async def listen(self):
        try:
            async for msg in self.consumer:
                try:
                    data = json.loads(msg.value)
                    yield msg.topic, data
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid JSON in message: {msg.value}")
        except Exception as e:
            self.logger.error(f"Error receiving messages from Kafka: {e}")
            raise

    async def close(self):
        if self.consumer:
            await self.consumer.stop()
            self.logger.info("AIOKafkaConsumer closed.")
