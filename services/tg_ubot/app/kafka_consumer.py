# File location: services/tg_ubot/app/kafka_consumer.py

import logging
from kafka import KafkaConsumer
from .config import settings
import asyncio
import json

logger = logging.getLogger("kafka_consumer")

class KafkaMessageConsumer:
    def __init__(self, topics, group_id='userbot_group'):
        """
        :param topics: список топиков, которые слушаем
        :param group_id: Kafka group_id
        """
        self.logger = logging.getLogger("kafka_consumer")
        self.topics = topics
        self.group_id = group_id
        self.consumer = None

    async def initialize(self):
        try:
            loop = asyncio.get_event_loop()
            def create_consumer():
                return KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    api_version=(2, 5, 0),
                    value_deserializer=lambda m: m.decode('utf-8')
                )
            self.consumer = await loop.run_in_executor(None, create_consumer)
            self.logger.info(f"KafkaConsumer создан и подключен к брокеру. Подписка на топики: {self.topics}")
        except Exception as e:
            self.logger.error(f"Ошибка при создании KafkaConsumer: {e}")
            raise

    async def listen(self):
        """
        Возвращает (topic, message_dict).
        """
        if self.consumer is None:
            raise Exception("Kafka consumer не инициализован.")
        loop = asyncio.get_event_loop()
        while True:
            try:
                record = await loop.run_in_executor(None, lambda: next(iter(self.consumer)))
                raw_value = record.value
                topic = record.topic
                try:
                    data = json.loads(raw_value)
                except json.JSONDecodeError:
                    self.logger.error(f"Получено некорректное JSON-сообщение: {raw_value}")
                    continue

                yield (topic, data)

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
