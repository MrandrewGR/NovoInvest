# app/utils.py

import asyncio
import random
import os
import logging
from zoneinfo import ZoneInfo
from datetime import datetime, time

from .config import settings

logger = logging.getLogger("utils")


def get_current_time_moscow():
    """
    Получить текущее время в часовом поясе Москвы.
    """
    return datetime.now(ZoneInfo("Europe/Moscow"))


def is_night_time():
    """
    Определить, является ли текущее время ночью в Москве.
    Ночью считается с 22:00 до 06:00.
    """
    current_time = get_current_time_moscow().time()
    return current_time >= time(22, 0) or current_time < time(6, 0)


def get_delay_settings(delay_type: str):
    """
    Получить минимальную и максимальную задержки в зависимости от времени суток и типа обработки.

    :param delay_type: Тип задержки ('chat' или 'channel').
    :return: Кортеж (delay_min, delay_max)
    """
    current_time = get_current_time_moscow()
    current_time_only = current_time.time()

    # Парсинг времени перехода из конфигурации
    transition_start_to_night = datetime.strptime(settings.TRANSITION_START_TO_NIGHT, "%H:%M").time()
    transition_end_to_night = datetime.strptime(settings.TRANSITION_END_TO_NIGHT, "%H:%M").time()
    transition_start_to_day = datetime.strptime(settings.TRANSITION_START_TO_DAY, "%H:%M").time()
    transition_end_to_day = datetime.strptime(settings.TRANSITION_END_TO_DAY, "%H:%M").time()

    # Переход к ночным задержкам
    if transition_start_to_night <= current_time_only < transition_end_to_night:
        # Вычисляем долю завершённости перехода
        start_dt = datetime.combine(current_time.date(), transition_start_to_night)
        end_dt = datetime.combine(current_time.date(), transition_end_to_night)
        total_seconds = (end_dt - start_dt).total_seconds()
        elapsed_seconds = (current_time - start_dt).total_seconds()
        fraction = elapsed_seconds / total_seconds

        if delay_type == "chat":
            min_delay = settings.CHAT_DELAY_MIN_DAY + fraction * (
                        settings.CHAT_DELAY_MIN_NIGHT - settings.CHAT_DELAY_MIN_DAY)
            max_delay = settings.CHAT_DELAY_MAX_DAY + fraction * (
                        settings.CHAT_DELAY_MAX_NIGHT - settings.CHAT_DELAY_MAX_DAY)
        elif delay_type == "channel":
            min_delay = settings.CHANNEL_DELAY_MIN_DAY + fraction * (
                        settings.CHANNEL_DELAY_MIN_NIGHT - settings.CHANNEL_DELAY_MIN_DAY)
            max_delay = settings.CHANNEL_DELAY_MAX_DAY + fraction * (
                        settings.CHANNEL_DELAY_MAX_NIGHT - settings.CHANNEL_DELAY_MAX_DAY)
        else:
            # Стандартные задержки, если тип неизвестен
            min_delay, max_delay = (1.0, 5.0)

        logger.debug(f"Переход к ночным задержкам ({delay_type}): min={min_delay}, max={max_delay}")
        return (min_delay, max_delay)

    # Переход к дневным задержкам
    elif transition_start_to_day <= current_time_only < transition_end_to_day:
        # Вычисляем долю завершённости перехода
        start_dt = datetime.combine(current_time.date(), transition_start_to_day)
        end_dt = datetime.combine(current_time.date(), transition_end_to_day)
        total_seconds = (end_dt - start_dt).total_seconds()
        elapsed_seconds = (current_time - start_dt).total_seconds()
        fraction = elapsed_seconds / total_seconds

        if delay_type == "chat":
            min_delay = settings.CHAT_DELAY_MIN_NIGHT + fraction * (
                        settings.CHAT_DELAY_MIN_DAY - settings.CHAT_DELAY_MIN_NIGHT)
            max_delay = settings.CHAT_DELAY_MAX_NIGHT + fraction * (
                        settings.CHAT_DELAY_MAX_DAY - settings.CHAT_DELAY_MAX_NIGHT)
        elif delay_type == "channel":
            min_delay = settings.CHANNEL_DELAY_MIN_NIGHT + fraction * (
                        settings.CHANNEL_DELAY_MIN_DAY - settings.CHANNEL_DELAY_MIN_NIGHT)
            max_delay = settings.CHANNEL_DELAY_MAX_NIGHT + fraction * (
                        settings.CHANNEL_DELAY_MAX_DAY - settings.CHANNEL_DELAY_MAX_NIGHT)
        else:
            # Стандартные задержки, если тип неизвестен
            min_delay, max_delay = (1.0, 5.0)

        logger.debug(f"Переход к дневным задержкам ({delay_type}): min={min_delay}, max={max_delay}")
        return (min_delay, max_delay)

    # Ночные задержки
    elif is_night_time():
        if delay_type == "chat":
            min_delay = settings.CHAT_DELAY_MIN_NIGHT
            max_delay = settings.CHAT_DELAY_MAX_NIGHT
        elif delay_type == "channel":
            min_delay = settings.CHANNEL_DELAY_MIN_NIGHT
            max_delay = settings.CHANNEL_DELAY_MAX_NIGHT
        else:
            min_delay, max_delay = (1.0, 5.0)

        logger.debug(f"Ночные задержки ({delay_type}): min={min_delay}, max={max_delay}")
        return (min_delay, max_delay)

    # Дневные задержки
    else:
        if delay_type == "chat":
            min_delay = settings.CHAT_DELAY_MIN_DAY
            max_delay = settings.CHAT_DELAY_MAX_DAY
        elif delay_type == "channel":
            min_delay = settings.CHANNEL_DELAY_MIN_DAY
            max_delay = settings.CHANNEL_DELAY_MAX_DAY
        else:
            min_delay, max_delay = (1.0, 5.0)

        logger.debug(f"Дневные задержки ({delay_type}): min={min_delay}, max={max_delay}")
        return (min_delay, max_delay)


async def human_like_delay(delay_min: float, delay_max: float):
    """
    Асинхронная задержка с заданным диапазоном.
    """
    delay = random.uniform(delay_min, delay_max)
    logger.debug(f"Задержка на {delay:.2f} секунд")
    await asyncio.sleep(delay)


def ensure_dir(path: str):
    """
    Создать директорию, если она не существует.
    """
    directory = os.path.dirname(path) if os.path.isfile(path) else path
    try:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Проверка/создание директории: {directory}")
    except Exception as e:
        logger.exception(f"Не удалось создать директорию '{directory}': {e}")
        raise
