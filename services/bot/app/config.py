# File location: ./services/bot/app/config.py

import os

# Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bot")
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "isin_in_portfolio")

FILES_DIR = os.getenv("FILES_DIR", "/app/files")

# Разрешённые пользователи (список user_id через запятую)
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "7079551").split(",")
