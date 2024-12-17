# app/logger.py

import logging
import os
from .utils import ensure_dir
from .config import settings

def setup_logging():
    # Основная директория для логов
    log_dir = "logs"
    ensure_dir(log_dir)

    # Настройка корневого логгера
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
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
        "utils": os.path.join(log_dir, "utils.log"),
        "userbot": os.path.join(log_dir, "userbot.log")
    }

    for logger_name, file_path in module_handlers.items():
        ensure_dir(os.path.dirname(file_path))
        file_handler = logging.FileHandler(file_path)
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
        file_handler.setFormatter(formatter)
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
        logger.addHandler(file_handler)

    # Основной логгер для пользовательского приложения
    userbot_logger = logging.getLogger("userbot")
    userbot_logger.addHandler(logging.FileHandler(module_handlers["userbot"]))
    userbot_logger.addHandler(logging.StreamHandler())

    return userbot_logger
