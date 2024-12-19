# File location: services/tg_standard_bot/app/config.py

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    KAFKA_BOOTSTRAP_SERVERS: str = Field("kafka:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_INSTRUCTION_TOPIC: str = Field("userbot_instructions", env="KAFKA_INSTRUCTION_TOPIC")

    LOG_FILE: str = Field("logs/bot.log", env="LOG_FILE")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")

    #class Config:
    #    env_file = ".env"
    #    env_file_encoding = 'utf-8'


settings = Settings()
