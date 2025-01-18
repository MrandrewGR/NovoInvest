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
    """
    A simple check if the current time is in [22:00, 06:00).
    """
    current_time = get_current_time_moscow().time()
    return current_time >= time(22, 0) or current_time < time(6, 0)


def compute_transition_fraction(current_time: datetime, start_time: time, end_time: time) -> float:
    """
    Calculates how far (0..1) current_time is between start_time and end_time.
    If end_time < start_time, this implies crossing midnight.
    """
    start_dt = current_time.replace(
        hour=start_time.hour, minute=start_time.minute,
        second=0, microsecond=0
    )
    end_dt = current_time.replace(
        hour=end_time.hour, minute=end_time.minute,
        second=0, microsecond=0
    )

    if end_dt < start_dt:
        end_dt += timedelta(days=1)

    total_seconds = (end_dt - start_dt).total_seconds()
    elapsed_seconds = (current_time - start_dt).total_seconds()
    fraction = elapsed_seconds / total_seconds if total_seconds != 0 else 0

    if fraction < 0:
        fraction = 0
    elif fraction > 1:
        fraction = 1

    return fraction


def interpolate_delays(day_min: float, day_max: float,
                       night_min: float, night_max: float,
                       fraction: float, direction: str = "day_to_night") -> tuple[float, float]:
    """
    Interpolates between day and night delay settings based on fraction (0..1).
    direction='day_to_night': fraction=0 => day, 1 => night
    direction='night_to_day': fraction=0 => night, 1 => day
    """
    if direction == "day_to_night":
        min_delay = day_min + fraction * (night_min - day_min)
        max_delay = day_max + fraction * (night_max - day_max)
    else:  # night_to_day
        min_delay = night_min + fraction * (day_min - night_min)
        max_delay = night_max + fraction * (day_max - night_max)
    return min_delay, max_delay


def get_delay_settings(delay_type: str):
    """
    Returns (min_delay, max_delay) depending on day/night and transition periods.
    The day/night boundaries and transition phases are from Settings.
    """
    current_time = get_current_time_moscow()
    current_time_only = current_time.time()

    start_night_t = datetime.strptime(settings.TRANSITION_START_TO_NIGHT, "%H:%M").time()  # e.g. 20:00
    end_night_t   = datetime.strptime(settings.TRANSITION_END_TO_NIGHT, "%H:%M").time()    # e.g. 22:00
    start_day_t   = datetime.strptime(settings.TRANSITION_START_TO_DAY, "%H:%M").time()    # e.g. 06:00
    end_day_t     = datetime.strptime(settings.TRANSITION_END_TO_DAY, "%H:%M").time()      # e.g. 08:00

    if delay_type == "chat":
        day_min, day_max = settings.CHAT_DELAY_MIN_DAY, settings.CHAT_DELAY_MAX_DAY
        night_min, night_max = settings.CHAT_DELAY_MIN_NIGHT, settings.CHAT_DELAY_MAX_NIGHT
    else:
        day_min, day_max = settings.CHANNEL_DELAY_MIN_DAY, settings.CHANNEL_DELAY_MAX_DAY
        night_min, night_max = settings.CHANNEL_DELAY_MIN_NIGHT, settings.CHANNEL_DELAY_MAX_NIGHT

    # Transition to night (20:00 - 22:00)
    if start_night_t <= current_time_only < end_night_t:
        fraction = compute_transition_fraction(current_time, start_night_t, end_night_t)
        min_delay, max_delay = interpolate_delays(day_min, day_max, night_min, night_max, fraction, "day_to_night")
        logger.debug(f"Transition to night: fraction={fraction:.2f}, min={min_delay:.2f}, max={max_delay:.2f}")
        return (min_delay, max_delay)

    # Transition to day (06:00 - 08:00)
    elif start_day_t <= current_time_only < end_day_t:
        fraction = compute_transition_fraction(current_time, start_day_t, end_day_t)
        min_delay, max_delay = interpolate_delays(day_min, day_max, night_min, night_max, fraction, "night_to_day")
        logger.debug(f"Transition to day: fraction={fraction:.2f}, min={min_delay:.2f}, max={max_delay:.2f}")
        return (min_delay, max_delay)

    # Full night
    elif is_night_time():
        logger.debug(f"Full night: min={night_min}, max={night_max}")
        return (night_min, night_max)

    # Full day
    else:
        logger.debug(f"Full day: min={day_min}, max={day_max}")
        return (day_min, day_max)


async def human_like_delay(delay_min: float, delay_max: float):
    """
    Sleeps for a random amount of time within [delay_min, delay_max].
    """
    delay = random.uniform(delay_min, delay_max)
    logger.debug(f"Sleeping for {delay:.2f} seconds (human-like delay)")
    await asyncio.sleep(delay)


def ensure_dir(path: str):
    """
    Ensure a directory exists for `path`. If `path` is a file, create its parent directory.
    """
    directory = os.path.dirname(path) if os.path.isfile(path) else path
    try:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Ensured directory: {directory}")
    except Exception as e:
        logger.exception(f"Could not create directory '{directory}': {e}")
        raise
