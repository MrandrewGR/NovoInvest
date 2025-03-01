# File location: ./services/xml_worker/app/config.py

import os

# Настройки подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "ni-kafka:9092")

# The code uses 'KAFKA_TOPIC' for consumption:
KAFKA_CONSUME_TOPIC = os.getenv("KAFKA_TOPIC", "ni-bot-in")

# The code uses 'UBOT_PRODUCE_TOPIC' for the producer:
KAFKA_PRODUCE_TOPIC = os.getenv("UBOT_PRODUCE_TOPIC", "ni-isin-out")

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
