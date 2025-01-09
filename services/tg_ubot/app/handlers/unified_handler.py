# services/tg_ubot/app/handlers/unified_handler.py

import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import (
    Message,
    MessageEntityUrl,
    MessageEntityTextUrl
)
from app.config import settings
from app.utils import human_like_delay, get_delay_settings, serialize_message, ensure_dir
import os

logger = logging.getLogger("unified_handler")

def register_unified_handler(client, message_buffer, counter, userbot_active, chat_id_to_target_id: dict):
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
        await process_message_event(event, "new_message", message_buffer, counter, client, chat_id_to_target_id)

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        if not userbot_active.is_set():
            return
        await process_message_event(event, "edited_message", message_buffer, counter, client, chat_id_to_target_id)

async def process_message_event(event, event_type, message_buffer, counter, client: TelegramClient, chat_id_to_target_id: dict):
    """
    Обработка входящего сообщения/редактированного сообщения.
    Собираем максимальный объём информации, сохраняем медиа (если есть),
    и формируем JSON для отправки в Kafka.
    """
    try:
        msg: Message = event.message
        # Задержка "по-человечески"
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

        chat_id = chat_info.get("chat_id")
        target_id = chat_id_to_target_id.get(chat_id)
        if target_id is None:
            logger.warning(f"Не найден target_id для chat_id {chat_id}")
            target_id = "unknown"  # Можно установить значение по умолчанию или обработать иначе

        month_part = msg.date.strftime('%Y-%m')

        # Собираем реакции (если Telethon поддерживает msg.reactions)
        reactions_info = []
        if getattr(msg, "reactions", None):
            if msg.reactions and msg.reactions.results:
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

        # Проверка, было ли отредактировано (для new_message msg.edit_date, как правило, None)
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Если есть forward
        forward_info = {}
        if msg.forward:
            forward_info = {
                "is_forwarded": True,
                "forwarded_from": str(msg.forward.from_name or ""),
                "forwarded_from_id": getattr(msg.forward.from_id, 'user_id', None),
                "forwarded_date": msg.forward.date.isoformat() if msg.forward.date else None
            }
            try:
                fwd_sender_entity = await msg.forward.get_chat()
                if fwd_sender_entity:
                    forward_info["forwarded_from_channel_id"] = getattr(fwd_sender_entity, 'id', None)
                    forward_info["forwarded_from_channel_title"] = getattr(fwd_sender_entity, 'title', "")
            except Exception as e:
                logger.error(f"Не удалось получить чат для пересланного сообщения: {e}")
        else:
            forward_info["is_forwarded"] = False

        # «Цитаты» внутри самого текста — по желанию, если нужно
        quotes = []
        raw_text = msg.raw_text or ""
        if raw_text:
            for line in raw_text.splitlines():
                if line.strip().startswith(">"):
                    quotes.append(line.strip())

        # --- НОВАЯ ЧАСТЬ: извлекаем ссылки + строим markdown-версию
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        # Формируем итоговый JSON, теперь с text_plain, text_markdown и links
        message_data = {
            "event_type": event_type,
            "message_id": msg.id,
            "date": msg.date.isoformat(),
            "month_part": month_part,

            "text_plain": raw_text,     # <--- «чистый» текст
            "text_markdown": text_markdown,  # <--- markdown-версия
            "links": links,            # <--- список словарей c offset/length/url/display_text

            "media_path": media_path,
            "reactions": reactions_info,
            "quotes": quotes,
            **reply_info,
            **forward_info,
            **edited_info,
            "sender": sender_info,
            "chat": chat_info,
            "target_id": target_id  # Устанавливаем target_id на основе chat_id
        }

        # Отправляем в общий topic tg_ubot_output (или тот, что задан в .env)
        kafka_topic = settings.KAFKA_UBOT_OUTPUT_TOPIC

        # Складываем в буфер (тема + словарь)
        await message_buffer.put((kafka_topic, message_data))
        logger.info(f"[unified_handler] Обработано сообщение {msg.id} из {chat_info.get('chat_title','')} с target_id {target_id}")
    except Exception as e:
        logger.exception(f"Ошибка в process_message_event: {e}")

def build_markdown_and_links(raw_text: str, entities: list):
    """
    Превращает исходный текст + entities в:
    1) Markdown-представление (где ссылки оформлены [текст](url))
    2) Список ссылок (offset, length, url, display_text)
    """
    # Если нет сущностей, возвращаем raw_text как есть и пустой массив
    if not entities:
        return raw_text, []

    # Финальная строка для markdown
    md_fragments = []
    # Список ссылок
    links = []

    last_offset = 0
    # Сортируем entities по offset
    entities_sorted = sorted(entities, key=lambda e: e.offset)

    for entity in entities_sorted:
        # Добавляем кусок текста от предыдущего offset до текущего entity.offset (без изменений)
        if entity.offset > last_offset:
            md_fragments.append(raw_text[last_offset:entity.offset])

        # Длина текущего entity
        e_length = entity.length
        # Фрагмент текста, на который указывает entity
        display_text = raw_text[entity.offset : entity.offset + e_length]

        if isinstance(entity, MessageEntityUrl):
            # URL, который виден напрямую в тексте
            url = display_text  # Само по себе является ссылкой
            # Добавляем в markdown [display_text](url)
            md_fragments.append(f"[{display_text}]({url})")
            # Добавляем в список ссылок
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text
            })
            last_offset = entity.offset + e_length

        elif isinstance(entity, MessageEntityTextUrl) and entity.url:
            # «Скрытая» ссылка, реальный url лежит в entity.url
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
            # Любая другая entity: можно обрабатывать по желанию
            # Пока просто добавляем исходный текст, без особой обработки
            md_fragments.append(display_text)
            last_offset = entity.offset + e_length

    # Добавляем остаток текста, который идёт после последней entity
    if last_offset < len(raw_text):
        md_fragments.append(raw_text[last_offset:])

    # Склеиваем всё в одну строку
    text_markdown = "".join(md_fragments)
    return text_markdown, links
