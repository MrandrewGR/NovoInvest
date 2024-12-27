import logging
import os
from .utils import ensure_dir
from .config import settings

def setup_logging():
    # Настройка корневого логгера с StreamHandler
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG),  # Установлен уровень DEBUG
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    # Список логгеров, которым необходимо добавить дополнительные обработчики (если нужно)
    module_loggers = [
        "handlers.chat_handler",
        "handlers.channel_handler",
        "kafka_producer",
        "kafka_consumer",
        "utils",
        "userbot",
        "state"
    ]

    for logger_name in module_loggers:
        try:
            logger = logging.getLogger(logger_name)
            logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG))
            # Удаляем все обработчики, чтобы избежать дублирования
            logger.handlers = []
            # Добавляем StreamHandler
            stream_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            logger.propagate = True  # Включена пропагация
        except Exception as e:
            print(f"Ошибка при настройке логгера {logger_name}: {e}")

    # Основной логгер для пользовательского приложения (userbot)
    userbot_logger = logging.getLogger("userbot")
    userbot_logger.propagate = True  # Включена пропагация

    # Тестовое сообщение
    logging.getLogger("userbot").info("Логирование настроено корректно.")

    return logging.getLogger("userbot")
