# services/tg_ubot/app/chat_info.py

import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel, ChatForbidden
from telethon.errors.rpcerrorlist import ChannelPrivateError

from .config import settings

logger = logging.getLogger("chat_info")


async def get_all_chats_info(client: TelegramClient):
    """
    Returns a dictionary chat_id_to_data (dict) where key = target_id and value = metadata:
    {
      target_id: {
        "target_id": target_id,
        "chat_title": ...,
        "chat_username": ...,
        "name_uname": ...,
        "entity_type": ...
      }, ...
    }

    Features:
    - For channels and supergroups, target_id = -100XXXXXXXXXX
    - For regular groups and users, target_id = entity.id
    - Excludes SavedMessages (777000), BotFather, Telegram, if specified in EXCLUDED_CHAT_IDS / EXCLUDED_USERNAMES.
    """
    chats_info = {}
    all_dialogs = await client.get_dialogs()

    excluded_ids = settings.EXCLUDED_CHAT_IDS or []
    excluded_unames = [u.lower() for u in (settings.EXCLUDED_USERNAMES or [])]

    for dialog in all_dialogs:
        entity = dialog.entity
        if not entity:
            continue

        raw_id = getattr(entity, 'id', None)
        if raw_id is None:
            continue

        raw_uname = getattr(entity, 'username', '') or ''
        if raw_uname.lower() in excluded_unames:
            logger.info(f"Excluding by username={raw_uname}, id={raw_id}")
            continue

        if raw_id in excluded_ids:
            logger.info(f"Excluding by chat_id={raw_id} (from EXCLUDED_CHAT_IDS)")
            continue

        target_id, entity_type = get_target_id_and_type(entity)
        if target_id is None:
            logger.debug(f"Could not determine target_id for raw_id={raw_id}, entity_type={entity_type}. Skipping.")
            continue

        chat_title = get_chat_title(entity)
        chat_username = getattr(entity, 'username', '') or ''
        name_uname = get_name_or_username(entity)

        chats_info[target_id] = {
            "target_id": target_id,
            "chat_title": chat_title,
            "chat_username": chat_username,
            "name_uname": name_uname,
            "entity_type": entity_type
        }
        logger.debug(f"Saved in chats_info[{target_id}]: {chats_info[target_id]}")

    logger.info(f"Total chats collected: {len(chats_info)}")
    return chats_info


def get_target_id_and_type(entity):
    """
    Returns (target_id, entity_type) with Telegram's format for channels/supergroups:
      - If Channel => -100XXXXXXXXXX
      - Otherwise, return entity.id as is.
    """
    if isinstance(entity, Channel):
        # Channel (broadcast) or supergroup (megagroup)
        if getattr(entity, 'broadcast', False) or getattr(entity, 'megagroup', False):
            str_id = f"-100{entity.id}"
            return int(str_id), "ChannelOrSupergroup"
        else:
            str_id = f"-100{entity.id}"
            return int(str_id), "UnknownChannelType"

    elif isinstance(entity, Chat):
        return entity.id, "Chat"

    elif isinstance(entity, User):
        return entity.id, "User"

    elif isinstance(entity, ChatForbidden):
        return None, "ChatForbidden"

    else:
        return None, "UnknownEntityType"


def get_chat_title(entity):
    """Returns title, or for User, concatenates FirstName + LastName."""
    if hasattr(entity, 'title'):
        return entity.title
    elif isinstance(entity, User):
        fn = entity.first_name or ""
        ln = entity.last_name or ""
        return (fn + " " + ln).strip()
    return ""


def get_name_or_username(entity):
    """Returns @username, or FirstName+LastName, or title, or Unknown."""
    uname = getattr(entity, 'username', '') or ''
    if uname:
        return "@" + uname

    if isinstance(entity, User):
        fn = entity.first_name or ""
        ln = entity.last_name or ""
        if fn or ln:
            return (fn + " " + ln).strip()
        return "Unknown"

    if hasattr(entity, 'title'):
        return entity.title

    return "Unknown"
