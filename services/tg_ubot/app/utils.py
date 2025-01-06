# File location: services/tg_ubot/app/utils.py

import asyncio
import random
import logging
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta
from .config import settings
import os

logger = logging.getLogger("utils")

def get_current_time_moscow():
    return datetime.now(ZoneInfo("Europe/Moscow"))

def is_night_time():
    """Простая проверка, попадаем ли в интервал 22:00—06:00."""
    current_time = get_current_time_moscow().time()
    return current_time >= time(22, 0) or current_time < time(6, 0)

def compute_transition_fraction(
    current_time: datetime,
    start_time: time,
    end_time: time
) -> float:
    """
    Считает, какой «процент» (0…1) пройден между start_time и end_time
    относительно current_time с учётом таймзоны.
    Делает корректировку, если end_dt < start_dt (т.е. переход через полночь).
    """

    # Привязываем start_dt и end_dt к ТЕКУЩЕЙ дате и таймзоне current_time
    start_dt = current_time.replace(
        hour=start_time.hour,
        minute=start_time.minute,
        second=0,
        microsecond=0
    )
    end_dt = current_time.replace(
        hour=end_time.hour,
        minute=end_time.minute,
        second=0,
        microsecond=0
    )

    # Если end_dt < start_dt, значит пересекаем полночь.
    # Для корректности добавим 1 день к end_dt.
    if end_dt < start_dt:
        end_dt += timedelta(days=1)

    total_seconds = (end_dt - start_dt).total_seconds()
    elapsed_seconds = (current_time - start_dt).total_seconds()

    # fraction не должен выходить за диапазон [0..1]
    fraction = elapsed_seconds / total_seconds
    if fraction < 0:
        fraction = 0
    elif fraction > 1:
        fraction = 1

    return fraction

def interpolate_delays(
    day_min: float,
    day_max: float,
    night_min: float,
    night_max: float,
    fraction: float,
    direction: str = "day_to_night"
) -> tuple[float, float]:
    """
    Возвращает (min_delay, max_delay), интерполируя от дневных к ночным или наоборот,
    в зависимости от direction.
    - direction='day_to_night': 0 -> day, 1 -> night
    - direction='night_to_day': 0 -> night, 1 -> day
    """
    if direction == "day_to_night":
        # fraction=0 => day, fraction=1 => night
        min_delay = day_min + fraction * (night_min - day_min)
        max_delay = day_max + fraction * (night_max - day_max)
    else:  # direction == "night_to_day"
        # fraction=0 => night, fraction=1 => day
        min_delay = night_min + fraction * (day_min - night_min)
        max_delay = night_max + fraction * (day_max - night_max)

    return min_delay, max_delay

def get_delay_settings(delay_type: str):
    """
    Универсальная логика: днём, ночью, и "переходные" интервалы (день->ночь, ночь->день).
    """
    current_time = get_current_time_moscow()
    current_time_only = current_time.time()

    # Читаем из настроек (times без даты, tz)
    start_night_t = datetime.strptime(settings.TRANSITION_START_TO_NIGHT, "%H:%M").time()  # 20:00
    end_night_t   = datetime.strptime(settings.TRANSITION_END_TO_NIGHT, "%H:%M").time()    # 22:00
    start_day_t   = datetime.strptime(settings.TRANSITION_START_TO_DAY, "%H:%M").time()    # 06:00
    end_day_t     = datetime.strptime(settings.TRANSITION_END_TO_DAY, "%H:%M").time()      # 08:00

    if delay_type == "chat":
        day_min, day_max = settings.CHAT_DELAY_MIN_DAY, settings.CHAT_DELAY_MAX_DAY
        night_min, night_max = settings.CHAT_DELAY_MIN_NIGHT, settings.CHAT_DELAY_MAX_NIGHT
    else:  # например, channel
        day_min, day_max = settings.CHANNEL_DELAY_MIN_DAY, settings.CHANNEL_DELAY_MAX_DAY
        night_min, night_max = settings.CHANNEL_DELAY_MIN_NIGHT, settings.CHANNEL_DELAY_MAX_NIGHT

    # Переход к ночи (20:00 - 22:00)
    if start_night_t <= current_time_only < end_night_t:
        fraction = compute_transition_fraction(current_time, start_night_t, end_night_t)
        min_delay, max_delay = interpolate_delays(
            day_min, day_max, night_min, night_max,
            fraction, direction="day_to_night"
        )
        logger.debug(f"Переход к ночи: fraction={fraction:.2f}, min={min_delay:.2f}, max={max_delay:.2f}")
        return (min_delay, max_delay)

    # Переход к дню (06:00 - 08:00)
    elif start_day_t <= current_time_only < end_day_t:
        fraction = compute_transition_fraction(current_time, start_day_t, end_day_t)
        min_delay, max_delay = interpolate_delays(
            day_min, day_max, night_min, night_max,
            fraction, direction="night_to_day"
        )
        logger.debug(f"Переход к дню: fraction={fraction:.2f}, min={min_delay:.2f}, max={max_delay:.2f}")
        return (min_delay, max_delay)

    # Полная ночь (22:00—06:00)
    elif is_night_time():
        logger.debug(f"Полная ночь: min={night_min}, max={night_max}")
        return (night_min, night_max)

    # Остальное время — полный день (08:00—20:00)
    else:
        logger.debug(f"Полный день: min={day_min}, max={day_max}")
        return (day_min, day_max)

async def human_like_delay(delay_min: float, delay_max: float):
    """
    Рандомная задержка в заданном диапазоне [delay_min, delay_max].
    """
    delay = random.uniform(delay_min, delay_max)
    logger.debug(f"Задержка на {delay:.2f} секунд")
    await asyncio.sleep(delay)

def ensure_dir(path: str):
    directory = os.path.dirname(path) if os.path.isfile(path) else path
    try:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Проверка/создание директории: {directory}")
    except Exception as e:
        logger.exception(f"Не удалось создать директорию '{directory}': {e}")
        raise

def serialize_message(message):
    """
    Recursively serialize a message object to a JSON-serializable format.
    """
    try:
        if isinstance(message, dict):
            return {k: serialize_message(v) for k, v in message.items()}
        elif isinstance(message, list):
            return [serialize_message(item) for item in message]
        elif isinstance(message, datetime):
            return message.isoformat()
        elif isinstance(message, bytes):
            return message.decode('utf-8', errors='replace')
        elif hasattr(message, 'to_dict'):
            return serialize_message(message.to_dict())
        else:
            return message
    except Exception as e:
        logger.error(f"Error during message serialization: {e}")
        return str(message)
