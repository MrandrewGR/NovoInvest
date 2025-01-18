# services/tg_ubot/app/send_test_message.py

import asyncio
import json
from kafka import KafkaProducer
from .config import settings
from .logger import setup_logging

logger = setup_logging()

def safe_json_serializer(message):
    try:
        return json.dumps(message).encode('utf-8')
    except (TypeError, ValueError) as e:
        logger.error(f"Error serializing test message: {e}")
        return None

async def send_test_message():
    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: safe_json_serializer(x)
    )
    message = {
        "id": 2,
        "chat_id": 67890,
        "date": "2024-12-27T22:10:00Z",
        "original_message": {
            "message": "Test message for Kafka!"
        },
        "downloaded_media": "/path/to/test/media"
    }
    future = producer.send('telegram_channel_messages', value=message)
    try:
        record_metadata = future.get(timeout=10)
        logger.info(f"Test message sent to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        logger.error(f"Failed to send test message: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(send_test_message())
