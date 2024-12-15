import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    API_ID = int(os.environ.get("TELEGRAM_API_ID"))
    API_HASH = os.environ.get("TELEGRAM_API_HASH")
    PHONE = os.environ.get("TELEGRAM_PHONE")

    # Идентификатор или username канала
    CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")

    # Идентификатор чата (числовой ID, отрицательное число для групп)
    CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID"))

    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Разные топики для канала и чата
    KAFKA_CHANNEL_TOPIC = os.environ.get("KAFKA_CHANNEL_TOPIC", "telegram_channel_messages")
    KAFKA_CHAT_TOPIC = os.environ.get("KAFKA_CHAT_TOPIC", "telegram_chat_messages")

    LOG_FILE = os.environ.get("LOG_FILE", "/app/logs/userbot.log")
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    MEDIA_DIR = os.environ.get("MEDIA_DIR", "media")
