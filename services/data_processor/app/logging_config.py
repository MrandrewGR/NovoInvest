# services/data_processor/app/logging_config.py

import logging
import os
from .utils import ensure_dir  # Убедитесь, что этот модуль существует и содержит функцию ensure_dir
from .config import settings

def setup_logging():
    """
    Настройка логирования для data_processor.
    """
    # Убедитесь, что директория для логов существует
    log_dir = os.path.dirname(settings.LOG_FILE)
    ensure_dir(log_dir)

    # Настройка корневого логгера с FileHandler и StreamHandler
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG),  # Уровень логирования
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[
            logging.FileHandler(settings.LOG_FILE),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)

    # Список логгеров, которым необходимо добавить дополнительные обработчики (если нужно)
    module_loggers = [
        "kafka_producer",
        "kafka_consumer",
        "database",
        "models",
        "utils",
        "consumer"  # Добавьте другие модули по необходимости
    ]

    for logger_name in module_loggers:
        try:
            logger = logging.getLogger(logger_name)
            logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG))
            # Удаляем все обработчики, чтобы избежать дублирования
            logger.handlers = []
            # Добавляем FileHandler
            file_handler = logging.FileHandler(settings.LOG_FILE)
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            # Добавляем StreamHandler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            logger.propagate = True  # Включена пропагация
        except Exception as e:
            print(f"Ошибка при настройке логгера {logger_name}: {e}")

    # Основной логгер для data_processor
    main_logger = logging.getLogger("data_processor")
    main_logger.propagate = True  # Включена пропагация

    # Тестовое сообщение
    main_logger.info("Логирование для data_processor настроено корректно.")

    return main_logger
