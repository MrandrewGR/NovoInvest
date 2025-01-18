# services/tg_ubot/app/logger.py

import logging
import os
from app.utils import ensure_dir
from .config import settings

def setup_logging():
    """
    Configure logging for the userbot application.
    """
    # Basic root logger with StreamHandler
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG),
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    module_loggers = [
        "unified_handler",
        "kafka_producer",
        "kafka_consumer",
        "utils",
        "userbot",
        "state",
        "chat_info",
        "backfill_manager",
        "gaps_manager",
        "kafka_instructions_consumer",
        "process_messages"
    ]

    for logger_name in module_loggers:
        try:
            logger = logging.getLogger(logger_name)
            logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.DEBUG))
            # Clear existing handlers (avoid duplicates)
            logger.handlers = []
            # Add our stream handler
            stream_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            logger.propagate = False
        except Exception as e:
            print(f"Error configuring logger {logger_name}: {e}")

    userbot_logger = logging.getLogger("userbot")
    userbot_logger.propagate = False

    logging.getLogger("userbot").info("Logging configured correctly.")
    return logging.getLogger("userbot")
