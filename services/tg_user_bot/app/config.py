# File location: services/tg_user_bot/app/config.py

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_PHONE: str
    TELEGRAM_CHANNEL_ID: str
    TELEGRAM_CHAT_ID: int

    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_CHANNEL_TOPIC: str = "telegram_channel_messages"
    KAFKA_CHAT_TOPIC: str = "telegram_chat_messages"
    KAFKA_INSTRUCTION_TOPIC: str = "userbot_instructions"

    LOG_FILE: str = "logs/userbot.log"
    LOG_LEVEL: str = "INFO"
    MEDIA_DIR: str = "media"

    TRANSITION_START_TO_NIGHT: str = "20:00"
    TRANSITION_END_TO_NIGHT: str = "22:00"
    TRANSITION_START_TO_DAY: str = "06:00"
    TRANSITION_END_TO_DAY: str = "08:00"

    CHAT_DELAY_MIN_DAY: float = 2.0
    CHAT_DELAY_MAX_DAY: float = 7.0
    CHAT_DELAY_MIN_NIGHT: float = 5.0
    CHAT_DELAY_MAX_NIGHT: float = 12.0

    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

    #class Config:
    #    env_file = ".env"
    #    env_file_encoding = 'utf-8'


settings = Settings()
