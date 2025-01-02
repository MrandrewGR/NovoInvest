# File location: ./services/xml_worker/app/config.py

import os

# Настройки подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CONSUME_TOPIC = os.getenv("KAFKA_TOPIC", "bot")  # Топик, откуда будем читать пути к XML-файлам
KAFKA_PRODUCE_TOPIC = os.getenv("KAFKA_PRODUCE_TOPIC", "isin_in_portfolio")  # Топик, куда будем отправлять ISIN

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
