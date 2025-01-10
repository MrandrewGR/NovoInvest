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
        :param target_ids: Список целевых ID чатов (и/или каналов, пользователей).
        """
        self.client = client
        self.target_ids = target_ids
        self.chats_info = {}  # dict {chat_id: {...}}

    async def fetch_chat_info(self):
        """
        Получает информацию о каждом целевом ID и заполняет chats_info.
        Для каждого ID из target_ids пытаемся вызвать get_entity,
        вычисляем chat_id с учётом -100 для каналов/супергрупп,
        и сохраняем результат в self.chats_info.
        """
        for original_id in self.target_ids:
            logger.debug(f"Обработка target_id={original_id}")
            try:
                entity = await self.client.get_entity(original_id)
                logger.debug(f"Получена сущность для ID {original_id}: {entity}")

                # Определяем итоговый chat_id и тип сущности (ChannelOrSupergroup, Chat, User и т.д.)
                chat_id, entity_type = self.get_chat_id_and_type(entity)
                logger.debug(f"chat_id={chat_id}, entity_type={entity_type} для ID {original_id}")

                if chat_id is None:
                    logger.warning(f"Не удалось вычислить chat_id для ID={original_id} (entity_type={entity_type})")
                    continue

                # Собираем дополнительные поля
                chat_title = self.get_chat_title(entity)
                chat_username = self.get_chat_username(entity)
                name_or_username = self.get_name_or_username(entity)

                # Формируем структуру
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

            except RPCError as e:
                logger.error(f"RPCError при получении информации для ID {original_id}: {e}")
            except Exception as e:
                logger.exception(f"Неизвестная ошибка для ID {original_id}: {e}")

    def get_chat_id_and_type(self, entity):
        """
        Возвращает (chat_id, entity_type), где:
          - Для каналов и супергрупп => chat_id = -100 * entity.id
          - Для обычных групп и пользователей => chat_id = entity.id
        """
        if isinstance(entity, Channel):
            # Канал (broadcast) или супергруппа (megagroup).
            # Telegram использует -100... для сообщений в каналах и супергруппах.
            if getattr(entity, 'broadcast', False) or getattr(entity, 'megagroup', False):
                return -100 * entity.id, "ChannelOrSupergroup"
            else:
                # Какая-то иная разновидность Channel
                return -100 * entity.id, "UnknownChannelType"
        elif isinstance(entity, Chat):
            # Обычная группа
            return entity.id, "Chat"
        elif isinstance(entity, User):
            # Пользователь (личный чат)
            return entity.id, "User"
        elif isinstance(entity, ChatForbidden):
            # Нет доступа к чату
            return None, "ChatForbidden"
        else:
            # На всякий случай для неизвестных типов
            return None, "UnknownEntityType"

    def get_chat_title(self, entity):
        """
        Получаем название чата/канала/пользователя (если есть).
        """
        if hasattr(entity, 'title'):
            return entity.title
        elif isinstance(entity, User):
            fn = entity.first_name or ""
            ln = entity.last_name or ""
            return f"{fn} {ln}".strip()
        else:
            return ""

    def get_chat_username(self, entity):
        """
        Возвращаем username, если он есть (у канала, группы или пользователя).
        """
        return getattr(entity, 'username', "") if hasattr(entity, 'username') else ""

    def get_name_or_username(self, entity):
        """
        Удобное "имя" для чата или пользователя:
          - Если есть username, возвращаем "@username"
          - Если это User, собираем "FirstName LastName"
          - Если это Chat/Channel и есть title, возвращаем его
          - Иначе "Unknown"
        """
        username = self.get_chat_username(entity)
        if username:
            return f"@{username}"
        elif isinstance(entity, User):
            fn = entity.first_name or ""
            ln = entity.last_name or ""
            return (fn + " " + ln).strip() or "Unknown"
        elif hasattr(entity, 'title'):
            return entity.title
        else:
            return "Unknown"

    def get_chats_info(self):
        """
        Возвращает весь словарь {chat_id: {...}}.
        """
        return self.chats_info


async def get_all_chats_info(client: TelegramClient):
    """
    Утилитная функция, чтобы снаружи быстро получить chats_info.
    """
    chat_info = ChatInfo(client, settings.TELEGRAM_TARGET_IDS)
    await chat_info.fetch_chat_info()
    logger.debug(f"Все чаты: {chat_info.get_chats_info()}")
    return chat_info.get_chats_info()


if __name__ == "__main__":
    import json
    import sys

    # Если хотите запускать отдельно как скрипт, можно так:
    logging.basicConfig(level=settings.LOG_LEVEL)

    async def main():
        # Подключаемся к TelegramClient
        client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
        await client.start()
        # Получаем список чатов
        chats = await get_all_chats_info(client)
        # Сохраняем
        with open("/app/logs/chats_info.json", "w", encoding="utf-8") as f:
            json.dump(list(chats.values()), f, ensure_ascii=False, indent=4)
        logger.info("Информация о чатах успешно сохранена в chats_info.json.")

    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Ошибка при выполнении chat_info.py: {e}")
        sys.exit(1)
