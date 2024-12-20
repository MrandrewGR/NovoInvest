# File location: services/tg_user_bot/app/kafka_consumer.py

import logging
import json
from aiokafka import AIOKafkaConsumer
from .config import settings

logger = logging.getLogger("kafka_consumer")

class KafkaMessageConsumer:
    def __init__(self):
        self.consumer = None

    async def initialize(self):
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_INSTRUCTION_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='userbot_group',
            value_deserializer=lambda m: m.decode('utf-8')
        )
        await self.consumer.start()
        logger.info("Kafka consumer инициализирован и слушает тему '%s'.", settings.KAFKA_INSTRUCTION_TOPIC)

    async def listen(self):
        try:
            async for message in self.consumer:
                yield message
        except Exception as e:
            logger.exception(f"Ошибка в Kafka consumer: {e}")
            raise

    async def close(self):
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer закрыт.")
