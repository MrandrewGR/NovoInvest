# File location: services/tg_user_bot/app/kafka_producer.py

import logging
from kafka import KafkaProducer
from .config import settings

class KafkaMessageProducer:
    def __init__(self):
        self.logger = logging.getLogger("kafka_producer")
        self.producer = None

    async def initialize(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 5, 0)
            )
            self.logger.info("KafkaProducer создан и подключен к брокеру.")
        except Exception as e:
            self.logger.error(f"Ошибка при создании KafkaProducer: {e}")
            raise

    async def send_message(self, topic, message):
        if self.producer is None:
            raise Exception("Kafka producer не инициализирован.")
        try:
            future = self.producer.send(topic, message.encode('utf-8'))
            record_metadata = future.get(timeout=10)
            self.logger.info(f"Сообщение отправлено в топик {record_metadata.topic}, партиция {record_metadata.partition}, смещение {record_metadata.offset}")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке сообщения: {e}")
            raise

    async def close(self):
        if self.producer:
            self.producer.close()
            self.logger.info("KafkaProducer закрыт.")
