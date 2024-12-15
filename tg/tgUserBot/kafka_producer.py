import logging
from kafka import KafkaProducer
import json

logger = logging.getLogger(__name__)

class KafkaMessageProducer:
    """
    Продюсер, позволяющий отправлять сообщения в разные топики Kafka.
    """
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        logger.info("Kafka producer initialized.")

    def send_message(self, topic: str, message: dict):
        """
        Отправить сообщение в указанный топик Kafka.
        """
        try:
            future = self.producer.send(topic, value=message)
            future.get(timeout=10)
            logger.debug("Message successfully sent to topic '%s': %s", topic, message.get('id'))
        except Exception as e:
            logger.exception("Failed to send message to Kafka: %s", e)

    def close(self):
        self.producer.close()
        logger.info("Kafka producer closed.")
