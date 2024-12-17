# app/config.py

from pydantic import BaseSettings, Field
from typing import List

class Settings(BaseSettings):
    # Telegram API Credentials
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_PHONE: str

    # Telegram Channel and Chat IDs
    TELEGRAM_CHANNEL_ID: str
    TELEGRAM_CHAT_ID: int

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CHANNEL_TOPIC: str = "telegram_channel_messages"
    KAFKA_CHAT_TOPIC: str = "telegram_chat_messages"

    # Logging Configuration
    LOG_FILE: str = "logs/userbot.log"
    LOG_LEVEL: str = "INFO"

    # Media Directory
    MEDIA_DIR: str = "media"

    # Delay Configuration (in seconds)
    # Chat Delays
    CHAT_DELAY_MIN_DAY: float = 2.0
    CHAT_DELAY_MAX_DAY: float = 7.0
    CHAT_DELAY_MIN_NIGHT: float = 5.0
    CHAT_DELAY_MAX_NIGHT: float = 12.0

    # Channel Delays
    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

    # Transition times (24-hour format)
    TRANSITION_START_TO_NIGHT: str = "20:00"  # 8:00 PM
    TRANSITION_END_TO_NIGHT: str = "22:00"    # 10:00 PM
    TRANSITION_START_TO_DAY: str = "06:00"    # 6:00 AM
    TRANSITION_END_TO_DAY: str = "08:00"      # 8:00 AM

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

# Инициализация настроек
settings = Settings()
