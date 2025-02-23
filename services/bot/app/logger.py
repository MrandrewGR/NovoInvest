# File location: ./services/tg_file_bot/app/logger.py
import logging
import sys

logger = logging.getLogger("tg_file_bot_logger")
logger.setLevel(logging.INFO)

# Хендлер вывода в stdout (виден в docker logs)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Формат вывода логов
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
