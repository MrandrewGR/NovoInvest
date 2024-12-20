# File location: services/tg_user_bot/app/logger.py

import logging
import os
from .utils import ensure_dir
from .config import settings
from logging.handlers import RotatingFileHandler

def setup_logging():
    # Используем переменную окружения для пути к логам
    log_dir = os.getenv("LOG_DIR", "/app/logs")
    ensure_dir(log_dir)

    # Настройка корневого логгера с StreamHandler
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG),  # Установлен уровень DEBUG
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    # Создание отдельных файлов логов для разных модулей
    module_handlers = {
        "handlers.chat_handler": os.path.join(log_dir, "chat", "chat_handler.log"),
        "handlers.channel_handler": os.path.join(log_dir, "channel", "channel_handler.log"),
        "kafka_producer": os.path.join(log_dir, "kafka_producer.log"),
        "kafka_consumer": os.path.join(log_dir, "kafka_consumer.log"),
        "utils": os.path.join(log_dir, "utils.log"),
        "userbot": os.path.join(log_dir, "userbot.log"),
        "state": os.path.join(log_dir, "state.log")
    }

    for logger_name, file_path in module_handlers.items():
        try:
            ensure_dir(os.path.dirname(file_path))
            file_handler = RotatingFileHandler(
                file_path,
                maxBytes=10*1024*1024,  # 10 MB
                backupCount=5
            )
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            file_handler.setFormatter(formatter)
            logger = logging.getLogger(logger_name)
            logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG))  # Установлен уровень DEBUG
            logger.addHandler(file_handler)
            logger.propagate = True  # Включена пропагация
        except Exception as e:
            print(f"Ошибка при настройке логгера {logger_name}: {e}")

    # Основной логгер для пользовательского приложения (userbot)
    userbot_logger = logging.getLogger("userbot")
    userbot_logger.propagate = True  # Включена пропагация

    # Тестовое сообщение
    logging.getLogger("userbot").info("Логирование настроено корректно.")

    return logging.getLogger("userbot")
