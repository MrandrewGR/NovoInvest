# File location: services/tg_user_bot/app/kafka_consumer.py

import logging
import asyncio
import json
from kafka import KafkaConsumer
from .config import settings

logger = logging.getLogger("kafka_consumer")

class KafkaMessageConsumer:
    def __init__(self):
        try:
            self.consumer = KafkaConsumer(
                settings.KAFKA_INSTRUCTION_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='userbot_group',
                value_deserializer=lambda m: m.decode('utf-8')
            )
            logger.info("Kafka consumer инициализирован и слушает тему '%s'.", settings.KAFKA_INSTRUCTION_TOPIC)
        except Exception as e:
            logger.exception(f"Не удалось инициализировать Kafka consumer: {e}")
            raise

    async def listen(self):
        loop = asyncio.get_running_loop()
        for message in self.consumer:
            yield message

    def close(self):
        try:
            self.consumer.close()
            logger.info("Kafka consumer закрыт.")
        except Exception as e:
            logger.exception(f"Ошибка при закрытии Kafka consumer: {e}")
