# File location: ./services/tg_file_bot/app/config.py
import os

# Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bot")
FILES_DIR = os.getenv("FILES_DIR", "/app/files")

ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "7079551").split(",")