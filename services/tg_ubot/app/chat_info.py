# services/tg_ubot/app/chat_info.py

import asyncio
import logging
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import User, Chat, Channel, ChatForbidden

from .config import settings

logger = logging.getLogger("chat_info")


class ChatInfo:
    """
    Класс для получения и хранения информации о целевых чатах и каналах.
    """

    def __init__(self, client: TelegramClient, target_ids: list[int]):
        self.client = client
        self.target_ids = target_ids
        self.chats_info = {}

    async def fetch_chat_info(self):
        """
        Получает информацию о каждом целевом ID и заполняет chats_info.
        """
        for original_id in self.target_ids:
            logger.debug(f"Обработка target_id={original_id}")
            try:
                entity = await self.client.get_entity(original_id)
                logger.debug(f"Получена сущность для ID {original_id}: {entity}")

                chat_id, entity_type = self.get_chat_id_and_type(entity)
                logger.debug(f"chat_id={chat_id}, entity_type={entity_type} для ID {original_id}")

                if chat_id is None:
                    logger.warning(f"Не удалось получить chat_id для ID {original_id} (entity_type={entity_type})")
                    continue

                chat_title = self.get_chat_title(entity)
                chat_username = self.get_chat_username(entity)
                name_or_username = self.get_name_or_username(entity)

                chat_data = {
                    "chat_id": chat_id,
                    "chat_title": chat_title,
                    "chat_username": chat_username,
                    "name_or_username": name_or_username,
                    "target_id": original_id,
                    "entity_type": entity_type
                }
                self.chats_info[chat_id] = chat_data
                logger.info(f"Получена информация для ID {original_id}: {chat_data}")

                # Дополнительный лог для проверки
                if original_id == 7079551:
                    logger.debug("Информация о chat_id=7079551 успешно добавлена в chats_info.")

            except RPCError as e:
                logger.error(f"RPCError при получении информации для ID {original_id}: {e}")
            except Exception as e:
                logger.exception(f"Неизвестная ошибка для ID {original_id}: {e}")

    def get_chat_id_and_type(self, entity):
        """
        Храним entity.id «как есть».
        """
        if isinstance(entity, Channel):
            if entity.broadcast:
                return entity.id, "Channel"
            elif entity.megagroup:
                return entity.id, "Supergroup"
            else:
                return entity.id, "UnknownChannelType"
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
        Получает название чата или канала.
        """
        if hasattr(entity, 'title'):
            return entity.title
        elif isinstance(entity, User):
            return f"{entity.first_name or ''} {entity.last_name or ''}".strip()
        else:
            return ""

    def get_chat_username(self, entity):
        """
        Получает username чата или канала.
        """
        return getattr(entity, 'username', "") if hasattr(entity, 'username') else ""

    def get_name_or_username(self, entity):
        """
        Получает название или username из сущности.
        """
        if self.get_chat_username(entity):
            return f"@{self.get_chat_username(entity)}"
        elif isinstance(entity, User):
            return f"{entity.first_name or ''} {entity.last_name or ''}".strip() or "Unknown"
        elif hasattr(entity, 'title'):
            return entity.title
        else:
            return "Unknown"

    def get_chats_info(self):
        """
        Возвращает сформированную информацию о чатах.
        """
        return self.chats_info


async def get_all_chats_info(client: TelegramClient):
    """
    Функция для получения информации о всех целевых чатах.
    """
    chat_info = ChatInfo(client, settings.TELEGRAM_TARGET_IDS)
    await chat_info.fetch_chat_info()
    logger.debug(f"Все чаты: {chat_info.get_chats_info()}")
    return chat_info.get_chats_info()


if __name__ == "__main__":
    import json

    logging.basicConfig(level=settings.LOG_LEVEL)

    async def main():
        client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
        await client.start()
        chats = await get_all_chats_info(client)
        # Сохраняем информацию в JSON-файл или выводим на экран
        with open("/app/logs/chats_info.json", "w", encoding="utf-8") as f:
            json.dump(list(chats.values()), f, ensure_ascii=False, indent=4)
        logger.info("Информация о чатах успешно сохранена в chats_info.json.")

    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Ошибка при выполнении chat_info.py: {e}")
