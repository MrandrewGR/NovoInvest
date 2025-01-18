# services/tg_ubot/app/process_messages.py

import re
import json
import logging
from datetime import datetime
from telethon.tl.types import Message, MessageEntityUrl, MessageEntityTextUrl

logger = logging.getLogger("process_messages")


def sanitize_table_name(name_uname):
    """
    Cleans the username for use in table names.
    Replaces non-alphanumeric characters with underscores and converts to lowercase.
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
    Returns a sanitized table name based on name_uname or target_id.
    Format: messages_{name_uname}
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
    Converts raw text and entities into (markdown_text, links).

    Returns:
      (text_markdown, links)
      where text_markdown is a string with [text](link) in Markdown
            links is a list of {offset, length, url, display_text}
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
    Serializes a Telethon Message into a JSON-serializable dictionary.

    - event_type: "new_message", "edited_message", "backfill_message", ...
    - chat_info: e.g. {"target_id":..., "chat_title":..., "name_uname":...}
    """
    try:
        logger.debug(f"[serialize_message] Serializing message_id={msg.id} from chat_id={msg.chat_id}")

        # Sender info
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
            logger.error(f"[serialize_message] Failed to get sender info: {e}")

        # Reactions
        reactions_info = []
        if getattr(msg, "reactions", None) and msg.reactions.results:
            for r in msg.reactions.results:
                emoticon = getattr(r.reaction, "emoticon", "")
                count = r.count
                reactions_info.append({"emoji": emoticon, "count": count})

        # Reply info
        reply_info = {}
        if msg.is_reply:
            try:
                reply_msg_id = msg.reply_to_msg_id
                reply_info = {
                    "is_reply": True,
                    "reply_to_msg_id": reply_msg_id,
                }
            except Exception as e:
                logger.error(f"[serialize_message] Failed to get reply message: {e}")

        # Edit info
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Forward info
        forward_info = {"is_forwarded": False}
        if msg.forward:
            forward_info["is_forwarded"] = True
            forward_info["forwarded_from"] = str(msg.forward.from_name or "")
            forwarded_from_id = getattr(msg.forward.from_id, "user_id", None)
            forward_info["forwarded_from_id"] = forwarded_from_id
            forward_info["forwarded_date"] = msg.forward.date.isoformat() if msg.forward.date else None

        # Quotes
        raw_text = msg.raw_text or ""
        quotes = [line.strip() for line in raw_text.splitlines() if line.strip().startswith(">")]

        # Markdown + links
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        # Media path - actual downloading is handled elsewhere
        media_path = None
        if msg.media:
            try:
                media_path = msg.media.to_dict().get('file', None)
            except Exception as e:
                logger.error(f"[serialize_message] Failed to process media: {e}")

        target_id = chat_info.get("target_id", "unknown")
        chat_title = chat_info.get("chat_title", "Untitled")
        name_uname = chat_info.get("name_uname", "Unknown")

        month_part = msg.date.strftime("%Y-%m")

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
            "chat_id": msg.chat_id,
            "chat_title": chat_title,
            "target_id": target_id,
            "name_uname": name_uname,
            "month_part": month_part,
        }

        logger.debug(f"[serialize_message] Serialized: {json.dumps(message_data, ensure_ascii=False)}")
        return message_data

    except Exception as e:
        logger.exception(f"[serialize_message] Error serializing message_id={msg.id}: {e}")
        return {}
