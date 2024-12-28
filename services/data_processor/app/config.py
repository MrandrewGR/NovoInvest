# File: services/data_processor/app/config.py
import os

class Settings:
    LOG_FILE = os.getenv("LOG_FILE", "/app/logs/data_processor.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/data_processor_db")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    # Тот самый новый топик:
    KAFKA_UBOT_OUTPUT_TOPIC = os.getenv("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")

settings = Settings()
