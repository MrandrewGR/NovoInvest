# services/data_processor/app/config.py

import os

class Settings:
    LOG_FILE = os.getenv("LOG_FILE", "/app/logs/data_processor.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/data_processor_db")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_channel_messages")
    # Добавьте другие настройки по необходимости

settings = Settings()
