# services/nsd_worker/app/utils.py

import asyncio
import random
import aiohttp
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

async def random_delay(min_seconds: int, max_seconds: int):
    """
    Sleep for a random number of seconds between min_seconds and max_seconds.
    """
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Sleeping for {delay:.2f} seconds before next fetch...")
    await asyncio.sleep(delay)

async def fetch_content(url: str) -> str:
    """
    Perform an async HTTP GET to fetch the content from the given URL.
    Returns the response body as text.
    """
    logger.info(f"Fetching URL: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                return text
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return ""


def parse_html(content):
    soup = BeautifulSoup(content, 'html.parser')
    # Extract the relevant data or process the content here
    return soup

def _element_to_dict(element) -> dict:
    """
    Recursively convert an lxml Element into a Python dict.
    """
    data_dict = {
        "tag": element.tag,
        "text": element.text.strip() if element.text else "",
        "children": []
    }
    # Attributes
    if element.attrib:
        data_dict["attributes"] = dict(element.attrib)

    # Child elements
    for child in element:
        data_dict["children"].append(_element_to_dict(child))

    return data_dict
