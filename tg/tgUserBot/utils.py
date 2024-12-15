import asyncio
import random
import os


async def human_like_delay(min_delay=0.1, max_delay=0.5):
    await asyncio.sleep(random.uniform(min_delay, max_delay))


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
