import logging
from kafka import KafkaProducer
import json

logger = logging.getLogger(__name__)

class KafkaMessageProducer:
    """
    Продюсер, позволяющий отправлять сообщения в разные топики Kafka.
    """
    def __init__(self, bootstrap_servers: str):
        try:
            logger.info("Initializing Kafka producer with bootstrap servers: %s", bootstrap_servers)
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5  # Увеличение количества попыток для подключения
            )
            logger.info("Kafka producer successfully initialized.")
        except Exception as e:
            logger.exception("Failed to initialize Kafka producer: %s", e)
            raise

    def send_message(self, topic: str, message: dict):
        """
        Отправить сообщение в указанный топик Kafka.
        """
        try:
            logger.debug("Sending message to topic '%s': %s", topic, message)
            future = self.producer.send(topic, value=message)
            future.get(timeout=10)
            logger.info("Message successfully sent to topic '%s': %s", topic, message.get('id'))
        except Exception as e:
            logger.exception("Failed to send message to Kafka: %s", e)

    def close(self):
        try:
            self.producer.close()
            logger.info("Kafka producer closed.")
        except Exception as e:
            logger.exception("Failed to close Kafka producer: %s", e)
