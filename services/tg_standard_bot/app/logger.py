# services/tg_standard_bot/app/logger.py

import logging
from services.tg_standard_bot.app.config import settings
import os


def setup_logging():
    os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(settings.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger("tg_standard_bot")
    return logger
