# services/tg_ubot/app/handlers/unified_handler.py

import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import Message
from .config import settings
from .utils import human_like_delay, get_delay_settings, serialize_message, ensure_dir
import os

logger = logging.getLogger("unified_handler")

def register_unified_handler(client, message_buffer, counter, userbot_active):
    """
    Регистрирует единый обработчик для всех "новых" и "изменённых" сообщений
    в списке целевых chat_id/каналов.
    """
    target_ids = settings.TELEGRAM_TARGET_IDS
    logger.info(f"Регистрируем unified_handler для чатов/каналов: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        if not userbot_active.is_set():
            return
        await process_message_event(event, "new_message", message_buffer, counter, client)

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        if not userbot_active.is_set():
            return
        await process_message_event(event, "edited_message", message_buffer, counter, client)

async def process_message_event(event, event_type, message_buffer, counter, client: TelegramClient):
    """
    Обработка входящего сообщения/редактированного сообщения.
    Собираем максимальный объём информации, сохраняем медиа (если есть),
    и формируем JSON для отправки в Kafka.
    """
    try:
        msg: Message = event.message
        # Задержка "по-человечески"
        # Определим, чат или канал → используем те же get_delay_settings,
        # можно условно назвать "chat" (или "channel"), или сделать логику по типу
        # (для простоты используем "chat")
        min_delay, max_delay = get_delay_settings("chat")
        await human_like_delay(min_delay, max_delay)

        # Инкремент счётчика
        await counter.increment()

        # Сохраняем медиа, если есть
        media_path = None
        if msg.media:
            ensure_dir(settings.MEDIA_DIR)
            try:
                media_path = await msg.download_media(file=settings.MEDIA_DIR)
            except Exception as e:
                logger.error(f"Не удалось скачать медиа: {e}")

        # Пробуем получить информацию об отправителе
        sender_info = {}
        try:
            sender = await msg.get_sender()
            if sender:
                sender_info = {
                    "sender_id": getattr(sender, "id", None),
                    "sender_username": getattr(sender, "username", ""),
                    "sender_first_name": getattr(sender, "first_name", ""),
                    "sender_last_name": getattr(sender, "last_name", "")
                }
        except Exception as e:
            logger.error(f"Не удалось получить информацию об отправителе: {e}")

        # Пробуем получить информацию о чате/канале
        chat_info = {}
        try:
            chat = await msg.get_chat()
            if chat:
                chat_info = {
                    "chat_id": getattr(chat, "id", None),
                    "chat_title": getattr(chat, "title", ""),
                    "chat_username": getattr(chat, "username", "")
                }
        except Exception as e:
            logger.error(f"Не удалось получить информацию о чате/канале: {e}")

        # Собираем реакции (если Telethon поддерживает msg.reactions)
        # В новых версиях Telethon: message.reactions.available_reactions, message.reactions.results
        # Но они могут быть None. Примерный код:
        reactions_info = []
        if getattr(msg, "reactions", None):
            if msg.reactions.results:
                for r in msg.reactions.results:
                    reaction_emoji = getattr(r.reaction, "emoticon", "")
                    count = r.count
                    reactions_info.append({"emoji": reaction_emoji, "count": count})

        # Является ли сообщение реплаем
        reply_info = {}
        if msg.is_reply:
            try:
                reply_msg = await msg.get_reply_message()
                if reply_msg:
                    reply_info = {
                        "is_reply": True,
                        "reply_to_msg_id": reply_msg.id,
                    }
            except Exception as e:
                logger.error(f"Не удалось получить reply-сообщение: {e}")

        # Проверка, было ли отредактировано (для new_message может быть msg.edit_date is None)
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Если есть forward
        forward_info = {}
        if msg.forward:
            forward_info = {
                "forwarded_from": str(msg.forward.from_name or ""),
                "forwarded_from_id": getattr(msg.forward.from_id, 'user_id', None),
                "forwarded_date": msg.forward.date.isoformat() if msg.forward.date else None
            }

        # «Цитаты» внутри самого текста — обычно вы парсите msg.raw_text на «> quote»,
        # но Telethon прямых полей для «цитат» не даёт.  Можете придумать условное правило.
        # Здесь — простой пример поиска строк, начинающихся на ">"
        quotes = []
        if msg.raw_text:
            for line in msg.raw_text.splitlines():
                if line.strip().startswith(">"):
                    quotes.append(line.strip())

        # Формируем итоговый JSON
        message_data = {
            "event_type": event_type,
            "message_id": msg.id,
            "date": msg.date.isoformat(),
            "text": msg.raw_text or "",
            "media_path": media_path,
            "reactions": reactions_info,
            "quotes": quotes,
            **reply_info,
            **forward_info,
            **edited_info,
            "sender": sender_info,
            "chat": chat_info,
        }

        # Отправляем в общий topic tg_ubot_output (или тот, что задан в .env)
        kafka_topic = settings.KAFKA_UBOT_OUTPUT_TOPIC

        # Складываем в буфер (тема + словарь)
        await message_buffer.put((kafka_topic, message_data))
        logger.info(f"[unified_handler] Обработано сообщение {msg.id} из {chat_info.get('chat_title','')}")

    except Exception as e:
        logger.exception(f"Ошибка в process_message_event: {e}")
