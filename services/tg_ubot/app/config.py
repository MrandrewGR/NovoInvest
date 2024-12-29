from pydantic import BaseSettings
from typing import List

class Settings(BaseSettings):
    # Список целевых чатов и каналов (как JSON-массив),
    # Docker Compose заполнит TELEGRAM_TARGET_IDS в окружении.
    TELEGRAM_TARGET_IDS: List[int]

    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_PHONE: str
    TELEGRAM_BOT_TOKEN: str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_UBOT_OUTPUT_TOPIC: str = "tg_ubot_output"

    # Логи
    LOG_LEVEL: str = "DEBUG"
    LOG_FILE: str = "/app/logs/userbot.log"
    MEDIA_DIR: str = "/app/media"

    # Переход между днём и ночью
    TRANSITION_START_TO_NIGHT: str = "20:00"
    TRANSITION_END_TO_NIGHT: str = "22:00"
    TRANSITION_START_TO_DAY: str = "06:00"
    TRANSITION_END_TO_DAY: str = "08:00"

    # Задержки для «днём» и «ночью»
    CHAT_DELAY_MIN_DAY: float = 2.0
    CHAT_DELAY_MAX_DAY: float = 7.0
    CHAT_DELAY_MIN_NIGHT: float = 5.0
    CHAT_DELAY_MAX_NIGHT: float = 12.0

    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

settings = Settings()