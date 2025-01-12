# File location: services/tg_ubot/app/chat_info.py

import asyncio
import logging
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import User, Chat, Channel, ChatForbidden

from .config import settings

logger = logging.getLogger("chat_info")


class ChatInfo:
    """
    Класс для получения и хранения информации о целевых чатах, каналах и супергруппах.
    """

    def __init__(self, client: TelegramClient, target_ids: list[int]):
        """
        :param client: Экземпляр TelegramClient
        :param target_ids: Список целевых ID (каналы, чаты, пользователи),
                          как задано в TELEGRAM_TARGET_IDS.
        """
        self.client = client
        self.target_ids = target_ids
        self.chats_info = {}  # Словарь вида {chat_id: {...}}

    async def fetch_chat_info(self):
        """
        Получает информацию о каждом ID из target_ids и заполняет self.chats_info.
        При этом:
          - Для каналов/супергрупп: chat_id = -100 * entity.id
          - Для обычных групп/пользователей: chat_id = entity.id (положительный)
        """
        for original_id in self.target_ids:
            logger.debug(f"Обработка target_id={original_id}")
            try:
                entity = await self.client.get_entity(original_id)
                logger.debug(f"Получена сущность для ID {original_id}: {entity}")

                chat_id, entity_type = self.get_chat_id_and_type(entity)
                logger.debug(f"Результат get_chat_id_and_type: chat_id={chat_id}, entity_type={entity_type}")

                if chat_id is None:
                    logger.warning(
                        f"Не удалось вычислить chat_id для ID={original_id}, entity_type={entity_type}. Пропускаем."
                    )
                    continue

                # Собираем название, username и т.д.
                chat_title = self.get_chat_title(entity)
                chat_username = self.get_chat_username(entity)
                name_or_username = self.get_name_or_username(entity)

                # Формируем структуру
                chat_data = {
                    "chat_id": original_id, # chat_id,
                    "chat_title": chat_title,
                    "chat_username": chat_username,
                    "name_or_username": name_or_username,
                    "target_id": original_id,
                    "entity_type": entity_type,
                }

                self.chats_info[chat_id] = chat_data
                logger.info(f"Получена информация для ID {original_id}: {chat_data}")

            except RPCError as e:
                logger.error(f"RPCError при получении информации для ID {original_id}: {e}")
            except Exception as e:
                logger.exception(f"Неизвестная ошибка для ID {original_id}: {e}")

    def get_chat_id_and_type(self, entity):
        """
        Возвращает (chat_id, entity_type):
          - Если канал или супергруппа => -100 * entity.id
          - Если обычный чат или пользователь => entity.id
        """
        if isinstance(entity, Channel):
            if entity.broadcast or entity.megagroup:
                return -100 * entity.id, "ChannelOrSupergroup"
            else:
                return -100 * entity.id, "UnknownChannelType"
        elif isinstance(entity, Chat):
            return entity.id, "Chat"
        elif isinstance(entity, User):
            return entity.id, "User"
        elif isinstance(entity, ChatForbidden):
            return None, "ChatForbidden"
        else:
            return None, "UnknownEntityType"

    def get_chat_title(self, entity):
        """
        Если это Chat/Channel - пытаемся взять title,
        если User - собираем "FirstName LastName", иначе пусто.
        """
        if hasattr(entity, 'title'):
            return entity.title
        elif isinstance(entity, User):
            fn = entity.first_name or ""
            ln = entity.last_name or ""
            return (fn + " " + ln).strip()
        else:
            return ""

    def get_chat_username(self, entity):
        """
        Если есть username, возвращаем. Иначе пустая строка.
        """
        return getattr(entity, 'username', "") or ""

    def get_name_or_username(self, entity):
        """
        Удобное «читаемое» имя:
         - Если есть username, "@username"
         - Если User, "FirstName LastName"
         - Если Chat/Channel, title
         - Иначе "Unknown"
        """
        username = self.get_chat_username(entity)
        logger.debug(f"get_name_or_username: username={username}")
        if username:
            return f"@{username}"
        elif isinstance(entity, User):
            fn = entity.first_name or ""
            ln = entity.last_name or ""
            name = (fn + " " + ln).strip()
            logger.debug(f"get_name_or_username: name={name}")
            return name if name else "Unknown"
        elif hasattr(entity, 'title'):
            title = entity.title
            logger.debug(f"get_name_or_username: title={title}")
            return title
        logger.debug("get_name_or_username: Unknown")
        return "Unknown"

    def get_chats_info(self):
        """
        Возвращает сформированный словарь {chat_id: {chat_title, ...}}.
        """
        return self.chats_info

async def get_all_chats_info(client: TelegramClient):
    """
    Утилитная функция, чтобы получить сформированный chats_info.
    """
    from .chat_info import ChatInfo
    logger = logging.getLogger("chat_info")

    chat_info = ChatInfo(client, settings.TELEGRAM_TARGET_IDS)
    await chat_info.fetch_chat_info()
    logger.debug(f"Все чаты: {chat_info.get_chats_info()}")
    return chat_info.get_chats_info()


if __name__ == "__main__":
    # Если вам нужно запускать этот файл отдельно:
    import json
    import sys
    import logging
    from telethon import TelegramClient
    from .config import settings

    logging.basicConfig(level=settings.LOG_LEVEL)

    async def main():
        client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
        await client.start()
        chats = await get_all_chats_info(client)
        with open("/app/logs/chats_info.json", "w", encoding="utf-8") as f:
            json.dump(list(chats.values()), f, ensure_ascii=False, indent=4)
        logger.info("Информация о чатах успешно сохранена в chats_info.json.")

    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Ошибка при выполнении chat_info.py: {e}")
        sys.exit(1)
