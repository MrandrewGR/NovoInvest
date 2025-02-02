# services/tg_ubot/app/config.py

"""
Конфигурация tg_ubot,
наследуемся от BaseConfig вашей библиотеки для Kafka/DB и т.д.
"""

import os
from pydantic import BaseSettings
from typing import Optional, List

# Импортируем BaseConfig из установленной библиотеки mirco_services_data_management
from mirco_services_data_management.config import BaseConfig


class TGUBotSettings(BaseSettings, BaseConfig):
    """
    Наследуемся от BaseConfig, которая содержит:
     - KAFKA_BROKER
     - DB_HOST/DB_NAME/...
     - POLL_INTERVAL_DB
    и т.д.
    """

    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str

    # Топики Kafka
    KAFKA_CONSUME_TOPIC: str = "unused"  # tg_ubot не читает внешние сообщения
    KAFKA_PRODUCE_TOPIC: str = os.getenv("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")

    # gap-scan
    KAFKA_GAP_SCAN_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_TOPIC", "gap_scan_request")
    KAFKA_GAP_SCAN_RESPONSE_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_RESPONSE_TOPIC", "gap_scan_response")

    # Настройки логирования
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG")

    # Исключаем чаты по ID/username
    EXCLUDED_CHAT_IDS: Optional[List[int]] = []
    EXCLUDED_USERNAMES: Optional[List[str]] = []

    # Настройки задержек (день/ночь)
    TRANSITION_START_TO_NIGHT: str = "20:00"
    TRANSITION_END_TO_NIGHT: str = "22:00"
    TRANSITION_START_TO_DAY: str = "06:00"
    TRANSITION_END_TO_DAY: str = "08:00"

    CHAT_DELAY_MIN_DAY: float = 1.0
    CHAT_DELAY_MAX_DAY: float = 3.0
    CHAT_DELAY_MIN_NIGHT: float = 2.0
    CHAT_DELAY_MAX_NIGHT: float = 6.0

    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

    # Telegram
    TELEGRAM_TARGET_IDS: Optional[List[int]] = []

    SESSION_FILE: str = "userbot.session"

    class Config:
        env_file = "/app/env/tg_ubot.env"
        env_file_encoding = "utf-8"


settings = TGUBotSettings()
