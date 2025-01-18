# services/tg_ubot/app/config.py

from pydantic import BaseSettings
from typing import Optional, List

class Settings(BaseSettings):
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_UBOT_OUTPUT_TOPIC: str = "tg_ubot_output"

    # Logs
    LOG_LEVEL: str = "DEBUG"
    LOG_FILE: str = "/app/logs/userbot.log"
    MEDIA_DIR: str = "/app/media"

    # Exclude certain chat IDs / usernames
    EXCLUDED_CHAT_IDS: Optional[List[int]] = []
    EXCLUDED_USERNAMES: Optional[List[str]] = []

    # Transition times for day/night
    TRANSITION_START_TO_NIGHT: str = "20:00"
    TRANSITION_END_TO_NIGHT: str = "22:00"
    TRANSITION_START_TO_DAY: str = "06:00"
    TRANSITION_END_TO_DAY: str = "08:00"

    # Delays
    CHAT_DELAY_MIN_DAY: float = 1.0
    CHAT_DELAY_MAX_DAY: float = 3.0
    CHAT_DELAY_MIN_NIGHT: float = 2.0
    CHAT_DELAY_MAX_NIGHT: float = 6.0

    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

    # Telegram target IDs (if empty, we won't register any handlers by default)
    TELEGRAM_TARGET_IDS: Optional[List[int]] = []

    # Session file path
    SESSION_FILE: str = "userbot.session"

settings = Settings()
