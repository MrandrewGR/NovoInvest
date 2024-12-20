# File location: services/tg_user_bot/app/kafka_consumer.py

import logging
from kafka import KafkaConsumer
from .config import settings

class KafkaMessageConsumer:
    def __init__(self):
        self.logger = logging.getLogger("kafka_consumer")
        self.consumer = None

    async def initialize(self):
        try:
            self.consumer = KafkaConsumer(
                settings.KAFKA_INSTRUCTION_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id='userbot_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 5, 0)
            )
            self.logger.info("KafkaConsumer создан и подключен к брокеру.")
        except Exception as e:
            self.logger.error(f"Ошибка при создании KafkaConsumer: {e}")
            raise

    async def listen(self):
        if self.consumer is None:
            raise Exception("Kafka consumer не инициализирован.")
        for message in self.consumer:
            yield message

    async def close(self):
        if self.consumer:
            self.consumer.close()
            self.logger.info("KafkaConsumer закрыт.")
