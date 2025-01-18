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
    # Remove leading '@'
    name_uname = name_uname.lstrip('@')
    # Replace all non-alphanumeric characters with '_'
    name_uname = re.sub(r'\W+', '_', name_uname)
    # Ensure the name starts with a letter or '_'
    if not re.match(r'^[A-Za-z_]', name_uname):
        name_uname = f"_{name_uname}"
    # Limit length to 63 characters (PostgreSQL limit)
    name_uname = name_uname[:63]
    return name_uname.lower()


def get_table_name(name_uname, target_id):
    """
    Returns a sanitized table name based on name_uname or target_id.
    Format: messages_{name_uname}
    """
    if name_uname and name_uname != "Unknown":
        sanitized = f"messages_{sanitize_table_name(name_uname)}"
    else:
        # If name_uname is absent, use target_id
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
            links is a list of dictionaries {offset, length, url, display_text}
    """
    if not entities:
        return raw_text, []

    md_fragments = []
    links = []
    last_offset = 0
    entities_sorted = sorted(entities, key=lambda e: e.offset)

    for entity in entities_sorted:
        # Copy intermediate text
        if entity.offset > last_offset:
            md_fragments.append(raw_text[last_offset:entity.offset])

        e_length = entity.length
        display_text = raw_text[entity.offset : entity.offset + e_length]

        if isinstance(entity, MessageEntityUrl):
            # Direct URL
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
            # Hidden URL
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
            # Other entities (bold, italic, mention, etc.) are not processed separately
            md_fragments.append(display_text)
            last_offset = entity.offset + e_length

    # Add the tail
    if last_offset < len(raw_text):
        md_fragments.append(raw_text[last_offset:])

    text_markdown = "".join(md_fragments)
    return text_markdown, links


def serialize_message(msg: Message, event_type: str, chat_info: dict) -> dict:
    """
    Serializes a Telethon Message into a JSON-serializable dictionary.

    Parameters:
    - msg: Telethon Message object
    - event_type: Type of event ("new_message", "edited_message", "backfill_message", etc.)
    - chat_info: Dictionary containing metadata about the chat

    Returns:
    - Dictionary representing the serialized message
    """
    try:
        logger.debug(f"[serialize_message] Serializing message_id={msg.id} from chat_id={msg.chat_id}")

        # Sender Information
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
                logger.debug(f"[serialize_message] Sender info: {sender_info}")
        except Exception as e:
            logger.error(f"[serialize_message] Failed to get sender info: {e}")

        # Reactions
        reactions_info = []
        if getattr(msg, "reactions", None) and msg.reactions.results:
            for r in msg.reactions.results:
                emoticon = getattr(r.reaction, "emoticon", "")
                count = r.count
                reactions_info.append({"emoji": emoticon, "count": count})

        # Reply Information
        reply_info = {}
        if msg.is_reply:
            try:
                reply_msg = msg.reply_to_msg_id  # Get the replied message ID
                reply_info = {
                    "is_reply": True,
                    "reply_to_msg_id": reply_msg,
                }
            except Exception as e:
                logger.error(f"[serialize_message] Failed to get reply message: {e}")

        # Edit Information
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Forward Information
        forward_info = {}
        if msg.forward:
            forward_info["is_forwarded"] = True
            forward_info["forwarded_from"] = str(msg.forward.from_name or "")
            forwarded_from_id = getattr(msg.forward.from_id, "user_id", None)
            forward_info["forwarded_from_id"] = forwarded_from_id
            forward_info["forwarded_date"] = msg.forward.date.isoformat() if msg.forward.date else None
        else:
            forward_info["is_forwarded"] = False

        # Quotes Extraction
        quotes = []
        raw_text = msg.raw_text or ""
        for line in raw_text.splitlines():
            if line.strip().startswith(">"):
                quotes.append(line.strip())

        # Markdown and Links
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        # Media Handling
        media_path = None
        if msg.media:
            try:
                media_path = msg.media.to_dict().get('file', None)
                # Note: Actual media downloading should be handled elsewhere
                logger.debug(f"[serialize_message] Media path: {media_path}")
            except Exception as e:
                logger.error(f"[serialize_message] Failed to process media: {e}")

        # Chat Information
        target_id = chat_info.get("target_id", "unknown")
        chat_title = chat_info.get("chat_title", "Untitled")
        name_uname = chat_info.get("name_uname", "Unknown")

        # Month Part for Partitioning
        month_part = msg.date.strftime("%Y-%m")

        # Final Serialized Message
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

        logger.debug(f"[serialize_message] Serialized message_data: {json.dumps(message_data, ensure_ascii=False)}")

        return message_data

    except Exception as e:
        logger.exception(f"[serialize_message] Error serializing message_id={msg.id}: {e}")
        return {}
