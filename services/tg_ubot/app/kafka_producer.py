# File location: services/tg_ubot/app/kafka_producer.py
import logging
from kafka import KafkaProducer
from .config import settings
import asyncio
import json

class KafkaMessageProducer:
    def __init__(self):
        self.logger = logging.getLogger("kafka_producer")
        self.producer = None

    async def initialize(self):
        try:
            loop = asyncio.get_event_loop()
            self.producer = await loop.run_in_executor(None, lambda: KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 5, 0),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            ))
            self.logger.info("KafkaProducer создан и подключен к брокеру.")
        except Exception as e:
            self.logger.error(f"Ошибка при создании KafkaProducer: {e}")
            raise

    async def send_message(self, topic, message):
        if self.producer is None:
            raise Exception("Kafka producer не инициализован.")
        try:
            loop = asyncio.get_event_loop()
            future = await loop.run_in_executor(None, lambda: self.producer.send(topic, message))
            record_metadata = await loop.run_in_executor(None, lambda: future.get(timeout=10))
            self.logger.info(f"Сообщение отправлено в топик {record_metadata.topic}, партиция {record_metadata.partition}, смещение {record_metadata.offset}")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке сообщения: {e}")
            raise

    async def close(self):
        if self.producer:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.producer.close)
            self.logger.info("KafkaProducer закрыт.")
