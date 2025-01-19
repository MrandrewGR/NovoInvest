# services/tg_ubot/app/process_messages.py

import re
import json
import logging
from datetime import datetime
from telethon.tl.types import Message, MessageEntityUrl, MessageEntityTextUrl
from zoneinfo import ZoneInfo  # Импортируем ZoneInfo для работы с часовыми поясами

logger = logging.getLogger("process_messages")


def sanitize_table_name(name_uname):
    """
    Очищает имя пользователя для использования в названиях таблиц.
    Заменяет неалфавитно-цифровые символы на подчеркивания и преобразует в нижний регистр.
    """
    if not isinstance(name_uname, str):
        name_uname = str(name_uname)
    name_uname = name_uname.lstrip('@')
    name_uname = re.sub(r'\W+', '_', name_uname)
    if not re.match(r'^[A-Za-z_]', name_uname):
        name_uname = f"_{name_uname}"
    return name_uname[:63].lower()


def get_table_name(name_uname, target_id):
    """
    Возвращает очищенное имя таблицы на основе name_uname или target_id.
    Формат: messages_{name_uname}
    """
    if name_uname and name_uname != "Unknown":
        sanitized = f"messages_{sanitize_table_name(name_uname)}"
    else:
        if target_id < 0:
            sanitized = f"messages_neg{abs(target_id)}"
        else:
            sanitized = f"messages_{target_id}"
    return sanitized.lower()


def build_markdown_and_links(raw_text: str, entities: list):
    """
    Преобразует сырой текст и сущности в (markdown_text, links).

    Возвращает:
      (text_markdown, links)
      где text_markdown — строка с [текст](ссылка) в Markdown
            links — список {offset, length, url, display_text}
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

        if isinstance(entity, MessageEntityUrl):
            url = display_text
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text,
            })
            last_offset = entity.offset + e_length
        elif isinstance(entity, MessageEntityTextUrl) and entity.url:
            url = entity.url
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text,
            })
            last_offset = entity.offset + e_length
        else:
            md_fragments.append(display_text)
            last_offset = entity.offset + e_length

    if last_offset < len(raw_text):
        md_fragments.append(raw_text[last_offset:])

    text_markdown = "".join(md_fragments)
    return text_markdown, links


def serialize_message(msg: Message, event_type: str, chat_info: dict) -> dict:
    """
    Сериализует сообщение Telethon в JSON-совместимый словарь.

    - event_type: "new_message", "edited_message", "backfill_message", ...
    - chat_info: например, {"target_id":..., "chat_title":..., "name_uname":...}
    """
    try:
        logger.debug(f"[serialize_message] Сериализация message_id={msg.id} из chat_id={msg.chat_id}")

        # Определяем московский часовой пояс
        moscow_tz = ZoneInfo("Europe/Moscow")

        # Преобразуем дату сообщения в московское время
        msg_date_moscow = msg.date.astimezone(moscow_tz)

        # Информация об отправителе
        sender_info = {}
        try:
            sender = msg.sender
            if sender:
                sender_info = {
                    "sender_id": getattr(sender, "id", None),
                    "sender_username": getattr(sender, "username", ""),
                    "sender_first_name": getattr(sender, "first_name", ""),
                    "sender_last_name": getattr(sender, "last_name", None),
                }
        except Exception as e:
            logger.error(f"[serialize_message] Не удалось получить информацию об отправителе: {e}")

        # Информация о реакциях
        reactions_info = []
        if getattr(msg, "reactions", None) and msg.reactions.results:
            for r in msg.reactions.results:
                emoticon = getattr(r.reaction, "emoticon", "")
                count = r.count
                reactions_info.append({"emoji": emoticon, "count": count})

        # Информация о реплае
        reply_info = {}
        if msg.is_reply:
            try:
                reply_msg_id = msg.reply_to_msg_id
                reply_info = {
                    "is_reply": True,
                    "reply_to_msg_id": reply_msg_id,
                }
            except Exception as e:
                logger.error(f"[serialize_message] Не удалось получить реплаем: {e}")

        # Информация об изменении
        edited_info = {}
        if msg.edit_date:
            try:
                edit_date_moscow = msg.edit_date.astimezone(moscow_tz)
                edited_info["was_edited"] = True
                edited_info["edit_date"] = edit_date_moscow.isoformat()
            except Exception as e:
                logger.error(f"[serialize_message] Не удалось преобразовать дату редактирования: {e}")

        # Информация о форварде
        forward_info = {"is_forwarded": False}
        if msg.forward:
            forward_info["is_forwarded"] = True
            forward_info["forwarded_from"] = str(msg.forward.from_name or "")
            forwarded_from_id = getattr(msg.forward.from_id, "user_id", None)
            forward_info["forwarded_from_id"] = forwarded_from_id
            try:
                forward_date_moscow = msg.forward.date.astimezone(moscow_tz) if msg.forward.date else None
                forward_info["forwarded_date"] = forward_date_moscow.isoformat() if forward_date_moscow else None
            except Exception as e:
                logger.error(f"[serialize_message] Не удалось преобразовать дату форварда: {e}")

        # Цитаты
        raw_text = msg.raw_text or ""
        quotes = [line.strip() for line in raw_text.splitlines() if line.strip().startswith(">")]

        # Markdown и ссылки
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        # Путь к медиа - загрузка обрабатывается отдельно
        media_path = None
        if msg.media:
            try:
                media_path = msg.media.to_dict().get('file', None)
            except Exception as e:
                logger.error(f"[serialize_message] Не удалось обработать медиа: {e}")

        target_id = chat_info.get("target_id", "unknown")
        chat_title = chat_info.get("chat_title", "Untitled")
        name_uname = chat_info.get("name_uname", "Unknown")

        month_part = msg_date_moscow.strftime("%Y-%m")

        message_data = {
            "event_type": event_type,
            "message_id": msg.id,
            "date": msg_date_moscow.isoformat(),  # Используем московское время
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
            "chat_id": msg.chat_id,
            "chat_title": chat_title,
            "target_id": target_id,
            "name_uname": name_uname,
            "month_part": month_part,
        }

        logger.debug(f"[serialize_message] Сериализовано: {json.dumps(message_data, ensure_ascii=False)}")
        return message_data

    except Exception as e:
        logger.exception(f"[serialize_message] Ошибка при сериализации message_id={msg.id}: {e}")
        return {}
