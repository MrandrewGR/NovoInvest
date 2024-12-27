# File location: services/tg_ubot/app/kafka_consumer.py

import logging
from kafka import KafkaConsumer
from .config import settings
import asyncio

class KafkaMessageConsumer:
    def __init__(self):
        self.logger = logging.getLogger("kafka_consumer")
        self.consumer = None

    async def initialize(self):
        try:
            loop = asyncio.get_event_loop()
            self.consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
                settings.KAFKA_INSTRUCTION_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id='userbot_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 5, 0)
            ))
            self.logger.info("KafkaConsumer создан и подключен к брокеру.")
        except Exception as e:
            self.logger.error(f"Ошибка при создании KafkaConsumer: {e}")
            raise

    async def listen(self):
        if self.consumer is None:
            raise Exception("Kafka consumer не инициализирован.")
        loop = asyncio.get_event_loop()
        while True:
            try:
                message = await loop.run_in_executor(None, lambda: next(iter(self.consumer)))
                yield message
            except StopIteration:
                self.logger.warning("Нет новых сообщений в Kafka.")
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Ошибка при получении сообщения из Kafka: {e}")
                raise

    async def close(self):
        if self.consumer:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.consumer.close)
            self.logger.info("KafkaConsumer закрыт.")
