# services/tg_ubot/app/config.py

from pydantic import BaseSettings, Field
import os


class Settings(BaseSettings):
    # Список целевых чатов и каналов из .env, разделённых запятыми
    TELEGRAM_TARGET_IDS: list[int] = Field(
        default_factory=lambda: list(
            map(int, filter(None, os.getenv("TELEGRAM_TARGET_IDS", "").replace(" ", "").split(",")))
        )
    )

    TELEGRAM_API_ID: int = Field(default_factory=lambda: int(os.getenv("TELEGRAM_API_ID", 0)))
    TELEGRAM_API_HASH: str = Field(default_factory=lambda: os.getenv("TELEGRAM_API_HASH", ""))
    TELEGRAM_PHONE: str = Field(default_factory=lambda: os.getenv("TELEGRAM_PHONE", ""))

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    # Обновляем или добавляем новый TOPIC для вывода
    KAFKA_UBOT_OUTPUT_TOPIC: str = Field(default=os.getenv("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output"))

    # Логи
    LOG_LEVEL: str = Field(default=os.getenv("LOG_LEVEL", "DEBUG"))
    LOG_FILE: str = Field(default=os.getenv("LOG_FILE", "/app/logs/userbot.log"))
    MEDIA_DIR: str = Field(default=os.getenv("MEDIA_DIR", "/app/media"))

    # Переход между днём и ночью
    TRANSITION_START_TO_NIGHT: str = Field(default=os.getenv("TRANSITION_START_TO_NIGHT", "20:00"))
    TRANSITION_END_TO_NIGHT: str = Field(default=os.getenv("TRANSITION_END_TO_NIGHT", "22:00"))
    TRANSITION_START_TO_DAY: str = Field(default=os.getenv("TRANSITION_START_TO_DAY", "06:00"))
    TRANSITION_END_TO_DAY: str = Field(default=os.getenv("TRANSITION_END_TO_DAY", "08:00"))

    # Задержки для «днём» и «ночью» (пример из старого кода)
    CHAT_DELAY_MIN_DAY: float = Field(default=float(os.getenv("CHAT_DELAY_MIN_DAY", 2.0)))
    CHAT_DELAY_MAX_DAY: float = Field(default=float(os.getenv("CHAT_DELAY_MAX_DAY", 7.0)))
    CHAT_DELAY_MIN_NIGHT: float = Field(default=float(os.getenv("CHAT_DELAY_MIN_NIGHT", 5.0)))
    CHAT_DELAY_MAX_NIGHT: float = Field(default=float(os.getenv("CHAT_DELAY_MAX_NIGHT", 12.0)))

    CHANNEL_DELAY_MIN_DAY: float = Field(default=float(os.getenv("CHANNEL_DELAY_MIN_DAY", 5.0)))
    CHANNEL_DELAY_MAX_DAY: float = Field(default=float(os.getenv("CHANNEL_DELAY_MAX_DAY", 10.0)))
    CHANNEL_DELAY_MIN_NIGHT: float = Field(default=float(os.getenv("CHANNEL_DELAY_MIN_NIGHT", 10.0)))
    CHANNEL_DELAY_MAX_NIGHT: float = Field(default=float(os.getenv("CHANNEL_DELAY_MAX_NIGHT", 20.0)))

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'


settings = Settings()
