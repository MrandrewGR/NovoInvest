# File location: ./services/xml_worker/app/config.py

import os

# Настройки подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Топик, откуда будем читать путь к XML-файлу + user_id
KAFKA_CONSUME_TOPIC = os.getenv("KAFKA_TOPIC", "bot")
# Топик, куда будем отправлять результат обработки (isin_in_portfolio)
KAFKA_PRODUCE_TOPIC = os.getenv("KAFKA_PRODUCE_TOPIC", "isin_in_portfolio")

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
