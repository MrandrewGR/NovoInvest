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
    Возвращает словарь chat_id_to_data (dict), где ключ = target_id, значение = метаданные:
    {
      target_id: {
        "target_id": target_id,
        "chat_title": ...,
        "chat_username": ...,
        "name_or_username": ...,
        "entity_type": ...
      }, ...
    }

    Особенности:
    - Для каналов и супергрупп делаем target_id = -100XXXXXXXXXX (конкатенация -100 + entity.id)
    - Для обычных групп и пользователей оставляем target_id = entity.id (положительное или небольшое отрицательное).
    - Исключаем SavedMessages (777000), BotFather, Telegram, если указано в EXCLUDED_CHAT_IDS / EXCLUDED_USERNAMES.
    """
    chats_info = {}
    all_dialogs = await client.get_dialogs()

    excluded_ids = settings.EXCLUDED_CHAT_IDS or []
    excluded_unames = [u.lower() for u in (settings.EXCLUDED_USERNAMES or [])]

    for dialog in all_dialogs:
        entity = dialog.entity
        if not entity:
            continue

        # Сырой (обычный) entity.id (целое число)
        raw_id = getattr(entity, 'id', None)
        if raw_id is None:
            continue

        # Проверка username на исключения
        raw_uname = getattr(entity, 'username', '') or ''
        if raw_uname.lower() in excluded_unames:
            logger.info(f"Исключаем по username={raw_uname}, id={raw_id}")
            continue

        if raw_id in excluded_ids:
            logger.info(f"Исключаем по chat_id={raw_id} (из EXCLUDED_CHAT_IDS)")
            continue

        # Определяем правильный target_id и тип
        target_id, entity_type = get_target_id_and_type(entity)
        if target_id is None:
            logger.debug(f"Не удалось вычислить target_id для raw_id={raw_id}, entity_type={entity_type}. Пропускаем.")
            continue

        # Получаем human-readable поля
        chat_title = get_chat_title(entity)
        chat_username = getattr(entity, 'username', '') or ''
        name_or_username = get_name_or_username(entity)

        chats_info[target_id] = {
            "target_id": target_id,
            "chat_title": chat_title,
            "chat_username": chat_username,
            "name_or_username": name_or_username,
            "entity_type": entity_type
        }
        logger.debug(f"Сохранено в chats_info[{target_id}]: {chats_info[target_id]}")

    logger.info(f"Всего собрано чатов: {len(chats_info)}")
    return chats_info


def get_target_id_and_type(entity):
    """
    Возвращаем (target_id, entity_type) с «телеграмным» форматом для каналов/супергрупп:
      - Если Channel (broadcast || megagroup) => -100XXXXXXXXXX (string concat),
      - Иначе возвращаем entity.id как есть.
    """
    if isinstance(entity, Channel):
        # Канал (broadcast) или супергруппа (megagroup)
        if getattr(entity, 'broadcast', False) or getattr(entity, 'megagroup', False):
            # Пример: entity.id = 1385413506 => target_id = -1001385413506
            str_id = f"-100{entity.id}"
            return int(str_id), "ChannelOrSupergroup"
        else:
            # Неизвестный тип канала
            str_id = f"-100{entity.id}"
            return int(str_id), "UnknownChannelType"

    elif isinstance(entity, Chat):
        # Обычная группа
        return entity.id, "Chat"

    elif isinstance(entity, User):
        # Пользователь
        return entity.id, "User"

    elif isinstance(entity, ChatForbidden):
        return None, "ChatForbidden"

    else:
        return None, "UnknownEntityType"


def get_chat_title(entity):
    """Вернём title, или для User соберём FirstName + LastName."""
    if hasattr(entity, 'title'):
        return entity.title
    elif isinstance(entity, User):
        fn = entity.first_name or ""
        ln = entity.last_name or ""
        return (fn + " " + ln).strip()
    return ""


def get_name_or_username(entity):
    """Вернём @username, или FName+LName, или title, или Unknown."""
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
