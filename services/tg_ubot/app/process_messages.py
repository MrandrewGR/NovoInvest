# services/tg_ubot/app/process_messages.py

"""
Сериализация сообщений Telethon -> JSON-совместимый dict
"""

import re
import json
import logging
from datetime import datetime
from telethon.tl.types import Message, MessageEntityUrl, MessageEntityTextUrl
from zoneinfo import ZoneInfo

logger = logging.getLogger("process_messages")


def build_markdown_and_links(raw_text: str, entities: list):
    """
    Преобразуем Text + Telethon entities => (Markdown, Links[]).
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

        if isinstance(entity, (MessageEntityUrl, MessageEntityTextUrl)):
            url = entity.url if isinstance(entity, MessageEntityTextUrl) else display_text
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text
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
    Сериализует Telethon Message в словарь:
      - date в московском времени
      - markdown, links
      - поля sender, chat_id, name_uname и т.п.
    """
    try:
        moscow_tz = ZoneInfo("Europe/Moscow")
        date_moscow = msg.date.astimezone(moscow_tz)

        sender_info = {}
        if msg.sender:
            sender_info = {
                "sender_id": getattr(msg.sender, "id", None),
                "sender_username": getattr(msg.sender, "username", ""),
                "sender_first_name": getattr(msg.sender, "first_name", ""),
                "sender_last_name": getattr(msg.sender, "last_name", ""),
            }

        raw_text = msg.raw_text or ""
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])
        month_part = date_moscow.strftime("%Y-%m")

        return {
            "event_type": event_type,
            "message_id": msg.id,
            "date": date_moscow.isoformat(),
            "text_plain": raw_text,
            "text_markdown": text_markdown,
            "links": links,
            "sender": sender_info,
            "chat_id": msg.chat_id,
            "chat_title": chat_info.get("chat_title", ""),
            "target_id": chat_info.get("target_id", ""),
            "name_uname": chat_info.get("name_uname", "Unknown"),
            "month_part": month_part
        }
    except Exception as e:
        logger.exception(f"[serialize_message] Error serializing message {msg.id}: {e}")
        return {}
