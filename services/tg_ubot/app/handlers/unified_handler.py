# File location: services/tg_ubot/app/handlers/unified_handler.py

import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import Message, MessageEntityUrl, MessageEntityTextUrl
from app.config import settings
from app.utils import human_like_delay, get_delay_settings, ensure_dir
import os

logger = logging.getLogger("unified_handler")


def register_unified_handler(client, message_buffer, counter, userbot_active, chat_id_to_data):
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
        await process_message_event(event, "new_message", message_buffer, counter, client, chat_id_to_data)

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        if not userbot_active.is_set():
            return
        await process_message_event(event, "edited_message", message_buffer, counter, client, chat_id_to_data)


async def process_message_event(event, event_type, message_buffer, counter, client, chat_id_to_data):
    """
    Обработка входящего сообщения/редактированного сообщения.
    """
    try:
        msg: Message = event.message
        logger.debug(f"Получено сообщение: {msg.id} из chat_id={msg.chat_id}")

        # Логируем список доступных chat_id
        logger.debug(f"Доступные chat_id в chats_info: {list(chat_id_to_data.keys())}")

        chat_id = msg.chat_id
        if chat_id not in chat_id_to_data:
            logger.warning(f"chat_id={chat_id} отсутствует в chat_id_to_data. Пропускаем.")
            return
        else:
            logger.debug(f"chat_id={chat_id} присутствует в chats_info.")

        # Задержка (днём/ночью)
        min_delay, max_delay = get_delay_settings("chat")
        await human_like_delay(min_delay, max_delay)

        # Инкремент счётчика сообщений
        await counter.increment()

        # Сохраняем медиа, если есть
        media_path = None
        if msg.media:
            ensure_dir(settings.MEDIA_DIR)
            try:
                media_path = await msg.download_media(file=settings.MEDIA_DIR)
                logger.debug(f"Медиа сохранено: {media_path}")
            except Exception as e:
                logger.error(f"Не удалось скачать медиа: {e}")

        # Информация об отправителе
        sender_info = {}
        try:
            sender = await msg.get_sender()
            if sender:
                sender_info = {
                    "sender_id": getattr(sender, "id", None),
                    "sender_username": getattr(sender, "username", ""),
                    "sender_first_name": getattr(sender, "first_name", ""),
                    "sender_last_name": getattr(sender, "last_name", None)
                }
                logger.debug(f"Информация об отправителе: {sender_info}")
        except Exception as e:
            logger.error(f"Не удалось получить информацию об отправителе: {e}")

        # Берём словарь, сохранённый в main.py
        chat_info = chat_id_to_data.get(chat_id)
        # например "target_id"
        target_id = chat_info.get("target_id", "unknown")
        chat_title = chat_info.get("chat_title", "Untitled")

        # Если нужно получить реальное msg.get_chat():
        #   real_chat = await msg.get_chat()

        # Пример: собираем реакции
        reactions_info = []
        if getattr(msg, "reactions", None):
            if msg.reactions.results:
                for r in msg.reactions.results:
                    emoticon = getattr(r.reaction, "emoticon", "")
                    count = r.count
                    reactions_info.append({"emoji": emoticon, "count": count})

        # Проверка reply
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

        # Проверка, было ли отредактировано
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Проверка forward
        forward_info = {}
        if msg.forward:
            forward_info["is_forwarded"] = True
            forward_info["forwarded_from"] = str(msg.forward.from_name or "")
            forward_info["forwarded_from_id"] = getattr(msg.forward.from_id, 'user_id', None)
            forward_info["forwarded_date"] = (msg.forward.date.isoformat() if msg.forward.date else None)
        else:
            forward_info["is_forwarded"] = False

        # Сбор «цитат»
        quotes = []
        raw_text = msg.raw_text or ""
        for line in raw_text.splitlines():
            if line.strip().startswith(">"):
                quotes.append(line.strip())

        # Извлечение ссылок + markdown-версию
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        name_or_username = chat_info.get("name_or_username")
        if not name_or_username or name_or_username == "Unknown":
            name_uname = target_id
        else:
            name_uname = name_or_username

        # Формируем итоговый JSON
        message_data = {
            "event_type": event_type,
            "message_id": msg.id,
            "date": msg.date.isoformat(),
            "text_plain": raw_text,
            "text_markdown": text_markdown,
            "links": links,
            "media_path": media_path,
            "reactions": reactions_info,
            "quotes": quotes,
            **reply_info,
            **edited_info,
            **forward_info,
            "sender": sender_info,
            "chat_id": chat_id,
            "chat_title": chat_title,
            "target_id": target_id,
            'name_uname': name_uname,
            'month_part': msg.date.strftime('%Y-%m')
        }

        # Отправляем в Kafka (или куда нужно)
        kafka_topic = settings.KAFKA_UBOT_OUTPUT_TOPIC
        await message_buffer.put((kafka_topic, message_data))
        logger.info(f"[unified_handler] Обработано сообщение {msg.id} из {chat_title}, target_id={target_id}")

    except Exception as e:
        logger.exception(f"Ошибка в process_message_event: {e}")


def build_markdown_and_links(raw_text: str, entities: list):
    """
    Превращает исходный текст + entities в (markdown-текст, список_ссылок).
    """
    if not entities:
        return raw_text, []

    md_fragments = []
    links = []
    last_offset = 0
    entities_sorted = sorted(entities, key=lambda e: e.offset)

    for entity in entities_sorted:
        if entity.offset > last_offset:
            md_fragments.append(raw_text[last_offset:entity.offset])

        e_length = entity.length
        display_text = raw_text[entity.offset : entity.offset + e_length]

        from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl
        if isinstance(entity, MessageEntityUrl):
            # URL виден напрямую
            url = display_text
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text
            })
            last_offset = entity.offset + e_length
        elif isinstance(entity, MessageEntityTextUrl) and entity.url:
            # скрытая ссылка
            url = entity.url
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text
            })
            last_offset = entity.offset + e_length
        else:
            # Остальные entity
            md_fragments.append(display_text)
            last_offset = entity.offset + e_length

    if last_offset < len(raw_text):
        md_fragments.append(raw_text[last_offset:])

    text_markdown = "".join(md_fragments)
    return text_markdown, links
